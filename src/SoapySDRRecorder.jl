module SoapySDRRecorder

using SoapySDR
using SigMF
using Unitful
using BufferedStreams

gc_bytes() = Base.gc_bytes()
get_time_us() = trunc(Int, time()*1_000_000)

"""
    record(output_file; direct_buffer_access = false, timer_display = true,
           device::Union{Nothing, SoapySDR.Device}=nothing, #XXX: Make this KWargs
           channels::Union{Nothing, AbstractArray{<:SoapySDR.Channel}}=nothing,
           channel_configuration::Union{Nothing, Function}=nothing)

Record data from a SDR device to a file or IOBuffer.
"""
function record(output::AbstractString;
                direct_buffer_access = false, timer_display = true,
                device::Union{Nothing, SoapySDR.Device}=nothing, #XXX: Make this KWargs
                channels::Union{Nothing, AbstractArray{<:SoapySDR.Channel}}=nothing,
                channel_configuration::Union{Nothing, Function}=nothing)

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
    num_channels = 1
    #num_channels = length(channels)

    # run the channel configuration function
    if channel_configuration !== nothing
        channel_configuration(device, channels)
    end

    # set up a stream (complex floats)
    format = Complex{Int16} #first(channels).native_stream_format
    rxStream = SoapySDR.Stream(format, channels)

    # create a re-usable buffer for rx samples
    buffsz = rxStream.mtu
    buffs = Ptr{format}[C_NULL] # pointer for direct buffer API
    # Allocate some buffers
    # concurrent ring buffer?
    # for some reason the compiler does not constant prop the type
    # TODO put in signature
    buffers = [Vector{Complex{Int16}}(undef, buffsz) for _ in 1:num_channels]

    @show rxStream.mtu
    byte_len = sizeof(format)*length(first(buffers))

    # output timings to console. Our austere TimerOutputs.jl
    last_timeoutput = get_time_us()

    # compute timeout based on sample_rate
    timeout_estimate = buffsz/first(channels).sample_rate*2

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

    # If there is timing slack, we can sleep a bit to run event handlers
    have_slack = true

    # Our bespoke TimerOutputs.jl implementation
    timers = [0,0,0]
    allocations = [0,0,0]
    temp_bytes = 0
    temp_time = 0

    handle = 0 # TODO match type to C return
    ready_to_write = false

    # Enable ther stream
    @info "streaming..."
    SoapySDR.activate!(rxStream)
    try
        while true
            begin
                temp_bytes = Base.gc_bytes()
                temp_time = get_time_us()
                if !direct_buffer_access
                    read!(rxStream, buffers, timeout=timeout_estimate)
                    ready_to_write = true
                else
                    err, handle, flags, timeNs =
                        SoapySDR.SoapySDRDevice_acquireReadBuffer(device, rxStream, buffs, timeout_estimate)
                    if err == SoapySDR.SOAPY_SDR_TIMEOUT
                        have_slack = true
                        continue # we don't have any data available yet, so loop
                    elseif err == SoapySDR.SOAPY_SDR_OVERFLOW
                        have_slack = false
                        err = buffsz # nothing to do, should be the MTU
                    end
                    @assert err > 0
                    buffers = unsafe_wrap(Array{format}, buffs[1], (buffsz,))
                end
                allocations[1] += Base.gc_bytes() - temp_bytes
                timers[1] = get_time_us() - temp_time
            end
            if ready_to_write
                temp_bytes = Base.gc_bytes()
                temp_time = get_time_us()
                for sample in buffers
                    # size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream)
                    ret = @ccall fwrite(pointer(sample)::Ptr{Complex{Int16}}, 4::Csize_t, length(sample)::Cint, io::Ptr{Cint})::Csize_t
                    if ret < 0
                        error("Error writing to file: $ret")
                    end
                end
                allocations[2] += Base.gc_bytes() - temp_bytes
                timers[2] = get_time_us() - temp_time
                ready_to_write = false
            end

            if have_slack && timer_display
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
            begin
                have_slack && GC.gc(false)
            end
        end
    finally
        SoapySDR.deactivate!(rxStream)
        @ccall close(io::Cint)::Cint
    end
end


end
