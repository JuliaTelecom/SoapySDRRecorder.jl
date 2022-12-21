module SoapySDRRecorder

using SoapySDR
using Unitful
using BufferedStreams
using GZip

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
                compress = false,
                compression_level = 0,
                stream_type::Type{T}=Complex{Int16}) where T

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

    # Allocate some buffers for each channel
    # for some reason the compiler does not constant prop the type
    # TODO put in signature
    buffers = [Vector{T}(undef, mtu) for _ in 1:num_channels]
    buff_ptrs = pointer(map(pointer, buffers))

    # output timings to console. Our austere TimerOutputs.jl
    last_timeoutput = get_time_us()

    # compute timeout based on sample_rate
    timeout_estimate = uconvert(u"μs", mtu/first(channels).sample_rate*2).val

    # assert some properties
    # all sample rates should be the same
    @assert all(c -> c.sample_rate == first(channels).sample_rate, channels)

    # open up the output file
    io = Ptr{Cint}[]
    compress_io = GZipStream[]
    for i in 1:num_channels
        output_base = abspath(output)*"."*string(i)*".dat"
        if !compress
            output = output_base
            touch(output)
            io_c = @ccall open(abspath(output)::Cstring, 1::Cint)::Cint
            if io_c < 0
                error("Error opening file, returned: $io_c")
            end
            # convert fd to stdio, it is faster
            push!(io, @ccall fdopen(io_c::Cint, "w"::Cstring)::Ptr{Cint})
        else
            push!(compress_io, GZip.open(output_base*".gz", "w"*string(compression_level)))
        end
    end

    # Our bespoke TimerOutputs.jl implementation
    timers = [0,0,0]
    allocations = [0,0,0]
    temp_bytes = 0
    temp_time = 0

    # Enable ther stream
    @info "streaming..."
    SoapySDR.activate!(rxStream)
    try
        @inbounds while true
            temp_bytes = Base.gc_bytes()
            temp_time = get_time_us()
            # collect list of pointers to pass to SoapySDR
            nread, out_flags, timens = SoapySDR.SoapySDRDevice_readStream(
                rxStream.d,
                rxStream,
                buff_ptrs,
                mtu,
                timeout_estimate,
            )
            if nread == SoapySDR.SOAPY_SDR_TIMEOUT
                @ccall printf("T"::Cstring)::Cint
                @ccall _flushlbf()::Cvoid
                continue
            elseif nread == SoapySDR.SOAPY_SDR_OVERFLOW
                # just keep going and set to MTU
                @ccall printf("O"::Cstring)::Cint
                @ccall _flushlbf()::Cvoid
                nread = mtu
            elseif nread < 0
                error("Error reading from stream: $(SoapySDR.SoapySDR_errToStr(nread))")
            end # else nread is the number of samples read, write to file next
            allocations[1] += Base.gc_bytes() - temp_bytes
            timers[1] += get_time_us() - temp_time

            temp_bytes = Base.gc_bytes()
            temp_time = get_time_us()
            for i in eachindex(buffers)
                sample = buffers[i]
                # size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream)
                if compress
                    write(compress_io[i], sample)
                else
                    ret = @ccall fwrite(pointer(sample)::Ptr{T}, sizeof(T)::Csize_t, nread::Cint, io[i]::Ptr{Cint})::Csize_t
                    if ret < 0
                        error("Error writing to file: $ret")
                    end
                end
            end
            allocations[2] += Base.gc_bytes() - temp_bytes
            timers[2] += get_time_us() - temp_time

            if timer_display
                temp_bytes = Base.gc_bytes()
                temp_time = get_time_us()
                begin
                    if get_time_us() - last_timeoutput > 1_000_000
                        telemetry_callback !== nothing && telemetry_callback(device, channels)
                        # some hackery to not allocate on the Julia GC so we use libc printf
                        @ccall printf("Read Stream: %ld μs, expected time: %ld μs, net allocations: %ld bytes\n"::Cstring, timers[1]::Int, timeout_estimate::Cint, allocations[1]::Int)::Cint
                        @ccall printf("Write File: %ld μs, net allocations: %ld bytes\n"::Cstring, timers[2]::Int, allocations[2]::Int)::Cint
                        @ccall printf("Telemetry: %ld μs, net allocations: %ld bytes\n"::Cstring, timers[3]::Int, allocations[3]::Int)::Cint
                        @ccall _flushlbf()::Cvoid
                        last_timeoutput = get_time_us()
                        timers .= 0
                    end
                end
                allocations[3] += Base.gc_bytes() - temp_bytes
                timers[3] += get_time_us() - temp_time
            end
        end
    finally
        SoapySDR.deactivate!(rxStream)
        @ccall close(io::Cint)::Cint
        compress && close(compress_io)
    end
end


end
