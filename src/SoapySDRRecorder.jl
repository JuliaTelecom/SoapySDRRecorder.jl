module SoapySDRRecorder

using SoapySDR
using Unitful
using BufferedStreams
using CodecZstd
using Base.Threads

gc_bytes() = Base.gc_bytes()
get_time_us() = trunc(Int, time()*1_000_000) #microseconds is a reasonable measure


function reader_task!(return_channel::Channel{T}, received_channel::Channel{T}, rxStream, mtu, timeout, num_timeouts, num_overflows, num_bufs_read) where T
    pt = eltype(eltype(T))
    ptrs = Vector{Ptr{pt}}(undef, rxStream.nchannels)
    while true
        buf = take!(return_channel)
        # collect list of pointers to pass to SoapySDR
        ret, out_flags, timens = SoapySDR.SoapySDRDevice_readStream(
            rxStream.d,
            rxStream,
            pointer(map!(pointer, ptrs, buf)),
            mtu,
            timeout,
        )
        if ret == SoapySDR.SOAPY_SDR_TIMEOUT
            #@ccall printf("T"::Cstring)::Cint
            #@ccall _flushlbf()::Cvoid
            num_timeouts[] += 1
            # put the buffer back into the queue
            put!(return_channel, buf)
            continue
        elseif ret == SoapySDR.SOAPY_SDR_OVERFLOW
            # just keep going and set to MTU
            #@ccall printf("O"::Cstring)::Cint
            #@ccall _flushlbf()::Cvoid
            num_overflows[] += 1
            num_bufs_read[] += 1
        elseif ret < 0
            error("Error reading from stream: $(SoapySDR.SoapySDR_errToStr(ret))")
        else
            num_bufs_read[] += 1
        end
        put!(received_channel, buf)
        GC.safepoint()
    end
end

"""
record(output::AbstractString;
                timer_display = true,
                device::Union{Nothing, SoapySDR.Device}=nothing, #XXX: Make this KWargs
                channels::Union{Nothing, AbstractArray{<:SoapySDR.Channel}}=nothing,
                channel_configuration::Union{Nothing, Function}=nothing,
                stream_type::Type{T}=Complex{Int16}) 

Record data from a SDR device..

"""
function record(output::AbstractString;
                timer_display = true,
                device::Union{Nothing, SoapySDR.Device}=nothing,
                channels::Union{Nothing, AbstractArray{<:SoapySDR.Channel}}=nothing,
                channel_configuration::Union{Nothing, Function}=nothing,
                telemetry_callback::Union{Nothing, Function}=nothing,
                csv_log_callback::Union{Nothing, Function}=nothing,
                csv_header_callback::Union{Nothing, Function}=nothing,
                timeout = nothing,
                compress = false,
                compression_level = 3,
                csv_log = true,
                stream_type::Type{T}=Complex{Int16},
                initial_buffers::Integer=64,
                array_pool_growth_factor::Integer=2) where T

    if Threads.nthreads() < 3
        error("This program requires at least 3 threads to run")
    end

    # open the first device
    device = if device === nothing
        devices = SoapySDR.Devices()
        if length(devices) == 0
            error("No devices found")
        else
            @info "No device specified, selecting first device available"
            device = Device(devices[1])
        end
    else
        device
    end

    channels = if channels === nothing
        device.rx
    else
        channels
    end
    num_channels = length(channels)

    # run the channel configuration function
    if channel_configuration !== nothing
        channel_configuration(device, channels)
    end

    # set up a stream (complex floats)
    rxStream = SoapySDR.Stream(T, channels)

    mtu = rxStream.mtu

    # compute timeout based on sample_rate, in integer microseconds for Soapy
    # note that poll has milisecond precision, so manually overriding this
    # for small MTU is sometimes required to avoid exesssive timeout reports
    timeout_estimate = if timeout === nothing
        trunc(Int, uconvert(u"μs", mtu/first(channels).sample_rate*2).val)
    elseif typeof(timeout) <: Unitful.AbstractQuantity
        trunc(Int, uconvert(u"μs", timeout).val)
    else
        trunc(Int,timeout)
    end

    # assert some properties
    # all sample rates should be the same
    @assert all(c -> c.sample_rate == first(channels).sample_rate, channels)

    # event counters and stats for stream reading
    num_bufs_read = Ref(0)
    num_timeouts = Ref(0)
    num_overflows = Ref(0)

    # open up the output file
    io = Ptr{Cint}[]
    csv_log_io = Ptr{Cint}(0)
    compress_io = ZstdCompressorStream[] # TODO type unstable

    # this channel is filled by reading off the SDR
    received_channel = Channel{Vector{Vector{T}}}(Inf)
    # this channel is the return-side once wrting is finished
    return_channel = Channel{Vector{Vector{T}}}(Inf)

    # Allocate some buffers for each channel
    for _ in 1:initial_buffers
        buf = [Vector{T}(undef, mtu) for _ in 1:num_channels]
        put!(return_channel, buf)
    end

    # we will keep track of the array pool size here
    array_pool_size = 0

    # This task will make sure there is always a buffer available
    # for the SDR to read into
    @info "Spawning buffer pool task..."
    pool_task = Threads.@spawn while true
        if isempty(return_channel)
            for _ in 1:nw_allocs
                buf = [Vector{T}(undef, mtu) for _ in 1:num_channels]
                put!(return_channel, buf)
            end
            array_pool_size += array_pool_size*(array_pool_growth_factor-1)
        end
        GC.safepoint()
    end

    # Intialize file outputs
    for i in 1:num_channels
        output_base = abspath(output)*"."*string(i)*".dat"
        if !compress
            touch(output_base)
            io_c = @ccall open(abspath(output_base)::Cstring, 1::Cint)::Cint
            if io_c < 0
                error("Error opening file, returned: $io_c")
            end
            # convert fd to stdio, it is faster
            push!(io, @ccall fdopen(io_c::Cint, "w"::Cstring)::Ptr{Cint})
        else
            push!(compress_io, ZstdCompressorStream(open(output_base*".zst", "w"), level=compression_level))
        end
    end
    if csv_log
        csv_log_file = abspath(output)*".csv"
        touch(csv_log_file)
        csv_log_io = @ccall open(csv_log_file::Cstring, 1::Cint)::Cint
        if csv_log_io < 0
            error("Error opening file, returned: $csv_log_io")
        end
        # convert fd to stdio, it is faster
        csv_log_io = @ccall fdopen(csv_log_io::Cint, "w"::Cstring)::Ptr{Cint}
        @ccall fprintf(csv_log_io::Ptr{Cint}, "time_us,num_bufs_read,num_overflows,num_timeouts,"::Cstring, get_time_us()::Int, num_bufs_read[]::Int, num_overflows[]::Int, num_timeouts[]::Int)::Cint
        csv_header_callback !== nothing && csv_header_callback(csv_log_io)
        @ccall fprintf(csv_log_io::Ptr{Cint}, "\n"::Cstring)::Cint
    end

    # output timings to console. Our austere TimerOutputs.jl
    last_timeoutput = get_time_us()
    last_csvoutput = get_time_us()

    # Enable ther stream
    @info "streaming..."
    SoapySDR.activate!(rxStream)
    try
        @info "Spawning reader task..."
        # SDR Reader Task
        sdr_reader = Threads.@spawn reader_task!(return_channel, received_channel, rxStream, mtu, timeout_estimate, num_timeouts, num_overflows, num_bufs_read)

        @info "Spawning file writer..."
        # File Writer Task
        file_writer = Threads.@spawn while true
            buf = take!(received_channel)
            for i in eachindex(buf)
                sample = buf[i]
                # size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream)
                if compress
                    write(compress_io[i], sample)
                    flush(compress_io[i])
                else
                    ret = @ccall fwrite(pointer(sample)::Ptr{T}, sizeof(T)::Csize_t, mtu::Cint, io[i]::Ptr{Cint})::Csize_t
                    if ret < 0
                        error("Error writing to file: $ret")
                    end
                    @ccall fflush(io[i]::Ptr{Cint})::Cint
                end
            end
            put!(return_channel, buf)
            GC.safepoint()
        end

        while true
            if csv_log
                # We will log some stats here, then let the callback add things.
                @ccall fprintf(csv_log_io::Ptr{Cint}, "%ld,%ld,%ld,%ld,"::Cstring, get_time_us()::Int, num_bufs_read[]::Int, num_overflows[]::Int, num_timeouts[]::Int)::Cint
                csv_log_callback !== nothing && csv_log_callback(csv_log_io, device, channels)
                @ccall fprintf(csv_log_io::Ptr{Cint}, "\n"::Cstring)::Cint
                @ccall fflush(csv_log_io::Ptr{Cint})::Cint
            end

            if timer_display
                telemetry_callback !== nothing && telemetry_callback(device, channels)
                # some hackery to not allocate on the Julia GC so we use libc printf
                @ccall printf("Number of Buffers read: %ld Number of overflows: %ld Number of timeouts: %ld Array pool size: %ld\n"::Cstring, num_bufs_read[]::Int, num_overflows[]::Int, num_timeouts[]::Int, array_pool_size::Int)::Cint
                @ccall _flushlbf()::Cvoid
            end
            sleep(1)
            istaskfailed(sdr_reader) && error("SDR Reader Task Failed")
            istaskfailed(file_writer) && error("File Writer Task Failed")
            istaskfailed(pool_task) && error("Array Pool Task Failed")

            # handle exit
            GC.safepoint()
        end
    finally
        SoapySDR.deactivate!(rxStream)
        for i in 1:num_channels
            @ccall close(io[i]::Cint)::Cint
            compress && close(compress_io[i])
        end
        @ccall close(csv_log_io::Cint)::Cint
    end
end


end
