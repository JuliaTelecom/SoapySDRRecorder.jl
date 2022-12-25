module SoapySDRRecorder

using SoapySDR
using Unitful
using BufferedStreams
using GZip
using Base.Threads

gc_bytes() = Base.gc_bytes()
get_time_us() = trunc(Int, time()*1_000_000) #microseconds is a reasonable measure

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
                device::Union{Nothing, SoapySDR.Device}=nothing, #XXX: Make this KWargs
                channels::Union{Nothing, AbstractArray{<:SoapySDR.Channel}}=nothing,
                channel_configuration::Union{Nothing, Function}=nothing,
                telemetry_callback::Union{Nothing, Function}=nothing,
                csv_log_callback::Union{Nothing, Function}=nothing,
                csv_header_callback::Union{Nothing, Function}=nothing,
                timeout = nothing,
                compress = false,
                compression_level = 0,
                csv_log = true,
                show_timer_stats = true,
                stream_type::Type{T}=Complex{Int16},
                initial_buffers=64) where T

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

    # Our bespoke TimerOutputs.jl implementation
    timers = [0,0,0,0]
    allocations = [0,0,0,0]
    temp_bytes = 0
    temp_time = 0

    # event counters and stats for stream reading
    num_bufs_read = 0
    num_timeouts = 0
    num_overflows = 0

    # open up the output file
    io = Ptr{Cint}[]
    csv_log_io = Ptr{Cint}(0)
    compress_io = GZipStream[]

    # this channel is filled by reading off the SDR
    received_channel = Channel{Tuple{Vector{Vector{T}}, Ptr{Ptr{T}}}}(Inf)
    # this channel is the return-side once wrting is finished
    return_channel = Channel{Tuple{Vector{Vector{T}}, Ptr{Ptr{T}}}}(Inf)

    # Allocate some buffers for each channel
    for _ in 1:initial_buffers
        buf = [Vector{T}(undef, mtu) for _ in 1:num_channels]
        ptrs = pointer(map(pointer, buf))
        put!(return_channel, (buf, ptrs))
    end

    # we will keep track of the array pool size here
    array_pool_size = initial_buffers

    # This task will make sure there is always a buffer available
    # for the SDR to read into
    @info "Spawning buffer pool task..."
    pool_task = Threads.@spawn while true
        if isempty(return_channel)
            buf = [Vector{T}(undef, mtu) for _ in 1:num_channels]
            ptrs = pointer(map(pointer, buf))
            put!(return_channel, (buf, ptrs))
            array_pool_size += 1
        end
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
            push!(compress_io, GZip.open(output_base*".gz", "w"*string(compression_level)))
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
        @ccall fprintf(csv_log_io::Ptr{Cint}, "time_us,num_bufs_read,num_overflows,num_timeouts,"::Cstring, get_time_us()::Int, num_bufs_read::Int, num_overflows::Int, num_timeouts::Int)::Cint
        csv_header_callback !== nothing && csv_header_callback(csv_log_io)
        @ccall fprintf(csv_log_io::Ptr{Cint}, "\n"::Cstring)::Cint
    end

    # output timings to console. Our austere TimerOutputs.jl
    last_timeoutput = get_time_us()
    last_csvoutput = get_time_us()

    have_slack = false
    write_buf = true

    # Enable ther stream
    @info "streaming..."
    SoapySDR.activate!(rxStream)
    try
        @info "Spawning reader task..."
        # SDR Reader Task
        sdr_reader = Threads.@spawn while true
            temp_bytes = Base.gc_bytes()
            temp_time = get_time_us()
            (buf, ptr) = take!(return_channel)
            # collect list of pointers to pass to SoapySDR
            nread, out_flags, timens = SoapySDR.SoapySDRDevice_readStream(
                rxStream.d,
                rxStream,
                ptr,
                mtu,
                timeout_estimate,
            )
            if nread == SoapySDR.SOAPY_SDR_TIMEOUT
                #@ccall printf("T"::Cstring)::Cint
                #@ccall _flushlbf()::Cvoid
                num_timeouts += 1
                # put the buffer back into the queue
                put!(return_channel, (buf, ptr))
                allocations[1] += Base.gc_bytes() - temp_bytes
                timers[1] += get_time_us() - temp_time
                continue
            elseif nread == SoapySDR.SOAPY_SDR_OVERFLOW
                # just keep going and set to MTU
                #@ccall printf("O"::Cstring)::Cint
                #@ccall _flushlbf()::Cvoid
                nread = mtu
                num_overflows += 1
                num_bufs_read += 1
            elseif nread < 0
                error("Error reading from stream: $(SoapySDR.SoapySDR_errToStr(nread))")
            else
                num_bufs_read += 1
            end

            # send the buffer to the file writer
            put!(received_channel, (buf, ptr))
            allocations[1] += Base.gc_bytes() - temp_bytes
            timers[1] += get_time_us() - temp_time
        end

        @info "Spawning file writer..."
        # File Writer Task
        file_writer = Threads.@spawn while true
            temp_bytes = Base.gc_bytes()
            temp_time = get_time_us()
            buf, _ = take!(received_channel)
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
            allocations[2] += Base.gc_bytes() - temp_bytes
            timers[2] += get_time_us() - temp_time
        end

        @info "Spawning logger..."
        # run this IO on the main thread
        logger = Threads.@spawn while true
            if csv_log
                begin
                    if get_time_us() - last_csvoutput > 1_000_000
                        temp_bytes = Base.gc_bytes()
                        temp_time = get_time_us()
                        # We will log some stats here, then let the callback add things.
                        @ccall fprintf(csv_log_io::Ptr{Cint}, "%ld,%ld,%ld,%ld,"::Cstring, get_time_us()::Int, num_bufs_read::Int, num_overflows::Int, num_timeouts::Int)::Cint
                        csv_log_callback !== nothing && csv_log_callback(csv_log_io, device, channels)
                        @ccall fprintf(csv_log_io::Ptr{Cint}, "\n"::Cstring)::Cint
                        @ccall fflush(csv_log_io::Ptr{Cint})::Cint
                        last_csvoutput = get_time_us()
                        if timer_display
                            allocations[3] += Base.gc_bytes() - temp_bytes
                            timers[3] += get_time_us() - temp_time
                        end
                    end
                end
            end

            if timer_display
                begin
                    if get_time_us() - last_timeoutput > 2_000_000
                        temp_bytes = Base.gc_bytes()
                        temp_time = get_time_us()
                        #telemetry_callback !== nothing && telemetry_callback(device, channels)
                        # some hackery to not allocate on the Julia GC so we use libc printf
                        if show_timer_stats
                            @ccall printf("Read Stream: %ld μs, net allocations: %ld bytes\n"::Cstring, timers[1]::Int, allocations[1]::Int)::Cint
                            @ccall printf("Write File: %ld μs, net allocations: %ld bytes\n"::Cstring, timers[2]::Int, allocations[2]::Int)::Cint
                            @ccall printf("CSV Log: %ld μs, net allocations: %ld bytes\n"::Cstring, timers[3]::Int, allocations[3]::Int)::Cint
                            @ccall printf("Telemetry: %ld μs, net allocations: %ld bytes\n"::Cstring, timers[4]::Int, allocations[4]::Int)::Cint
                        end
                        @ccall printf("Number of Buffers read: %ld Number of overflows: %ld Array Pool Size: %ld\n"::Cstring, num_bufs_read::Int, num_overflows::Int, array_pool_size::Int)::Cint
                        @ccall _flushlbf()::Cvoid
                        last_timeoutput = get_time_us()
                        timers .= 0
                        allocations[4] += Base.gc_bytes() - temp_bytes
                        timers[4] += get_time_us() - temp_time
                    end
                end
            end
        end
        wait(sdr_reader)
        wait(file_writer)
        wait(logger)
        wait(pool_task)
    finally
        SoapySDR.deactivate!(rxStream)
        for i in 1:num_channels
            @ccall close(io[i]::Cint)::Cint
            compress && close(compress_io[i])
        end
        @ccall close(csv_log_io[i]::Cint)::Cint
    end
end


end
