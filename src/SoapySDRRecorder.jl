module SoapySDRRecorder

using SoapySDR
using Unitful
using BufferedStreams

gc_bytes() = Base.gc_bytes()
get_time_us() = trunc(Int, time()*1_000_000) #microseconds is a reasonable measure

"""
    record(output_file; direct_buffer_access = false, timer_display = true,
           device::Union{Nothing, SoapySDR.Device}=nothing, #XXX: Make this KWargs
           channels::Union{Nothing, AbstractArray{<:SoapySDR.Channel}}=nothing,
           channel_configuration::Union{Nothing, Function}=nothing)

Record data from a SDR device to a file or IOBuffer.
"""
function record(output::AbstractString;
                timer_display = true,
                device::Union{Nothing, SoapySDR.Device}=nothing, #XXX: Make this KWargs
                channels::Union{Nothing, AbstractArray{<:SoapySDR.Channel}}=nothing,
                channel_configuration::Union{Nothing, Function}=nothing,
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
    touch(abspath(output))
    io = @ccall open(abspath(output)::Cstring, 1::Cint)::Cint
    if io < 0
        error("Error opening file, returned: $io")
    end
    # convert fd to stdio, it is faster
    io = @ccall fdopen(io::Cint, "w"::Cstring)::Ptr{Cint}

    # Our bespoke TimerOutputs.jl implementation
    timers = [0,0,0]
    allocations = [0,0,0]
    temp_bytes = 0
    temp_time = 0

    # Enable ther stream
    @info "streaming..."
    SoapySDR.activate!(rxStream)
    try
        while true
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
            allocations[1] += Base.gc_bytes() - temp_bytes
            timers[1] = get_time_us() - temp_time

            temp_bytes = Base.gc_bytes()
            temp_time = get_time_us()
            for sample in buffers
                # size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream)
                ret = @ccall fwrite(pointer(sample)::Ptr{T}, sizeof(T)::Csize_t, nread::Cint, io::Ptr{Cint})::Csize_t
                if ret < 0
                    error("Error writing to file: $ret")
                end
            end
            allocations[2] += Base.gc_bytes() - temp_bytes
            timers[2] = get_time_us() - temp_time

            if timer_display
                temp_bytes = Base.gc_bytes()
                temp_time = get_time_us()
                begin
                    if get_time_us() - last_timeoutput > 1_000_000
                        # some hackery to not allocate on the Julia GC so we use libc printf
                        @ccall printf("Read Stream: %ld μs, allocations: %ld bytes\n"::Cstring, timers[1]::Int, allocations[1]::Int)::Cint
                        @ccall printf("Write File: %ld μs, allocations: %ld bytes\n"::Cstring, timers[2]::Int, allocations[2]::Int)::Cint
                        @ccall printf("Telemetry: %ld μs, allocations: %ld bytes\n"::Cstring, timers[3]::Int, allocations[3]::Int)::Cint
                        @ccall _flushlbf()::Cvoid
                        last_timeoutput = get_time_us()
                    end
                end
                allocations[3] += Base.gc_bytes() - temp_bytes
                timers[3] = get_time_us() - temp_time
            end
        end
    finally
        SoapySDR.deactivate!(rxStream)
        @ccall close(io::Cint)::Cint
    end
end


end
