module SoapySDRRecorder

using SoapySDR
using SigMF
using Unitful
using BufferedStreams

gc_bytes() = Base.gc_bytes()
get_time_ms() = trunc(Int, time() * 1000)

"""
    record(output_file; direct_buffer_access = false, timer_display = true,
           device::Union{Nothing, SoapySDR.Device}=nothing, #XXX: Make this KWargs
           channels::Union{Nothing, AbstractArray{<:SoapySDR.Channel}}=nothing,
           channel_configuration::Union{Nothing, Function}=nothing)

Record data from a SDR device to a file or IOBuffer.
"""
function record(output::Union{IO, AbstractString};
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
    num_channels = length(channels)

    # run the channel configuration function
    if channel_configuration !== nothing
        channel_configuration(device, channels)
    end

    # set up a stream (complex floats)
    format = Complex{Int16} #first(channels).native_stream_format
    rxStream = SoapySDR.Stream(format, channels)

    # create a re-usable buffer for rx samples
    buffsz = rxStream.mtu
    buff = Array{format}(undef, buffsz)
    buffs = Ptr{format}[C_NULL] # pointer for direct buffer API
    # Allocate some buffers
    # concurrent ring buffer?
    num_buffers = 16 
    buffers = [ntuple(_ -> Vector{format}(undef, buffsz), num_channels) for _ in 1:num_buffers]

    last_timeoutput = get_time_ms()

    # compute timeout based on sample_rate
    timeout_estimate = buffsz/first(channels).sample_rate*2

    # assert some properties
    # all sample rates should be the same
    @assert all(c -> c.sample_rate == first(channels).sample_rate, channels)

    @show timeout_estimate

    io = BufferedOutputStream(if output isa AbstractString
        open(output, "w")
    elseif output isa IO
        output
    end)

    # If there is timing slack, we can sleep a bit to run event handlers
    have_slack = true

    timers = [0,0,0]
    allocations = [0,0,0]
    temp_bytes = 0
    temp_time = 0

    # Enable ther stream
    @info "streaming..."
    SoapySDR.activate!(rxStream)
    try
        while true
            begin
                temp_bytes = Base.gc_bytes()
                temp_time = get_time_ms()
                if !direct_buffer_access
                    read!(rxStream, buffers[1], timeout=timeout_estimate)
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
                    buff = unsafe_wrap(Array{format}, buffs[1], (buffsz,))
                    #XXX: We probably should return after we finish the write to file
                    SoapySDR.SoapySDRDevice_releaseReadBuffer(device, rxStream, handle)
                end
                allocations[1] += Base.gc_bytes() - temp_bytes
                timers[1] = get_time_ms() - temp_time
            end
            begin
                temp_bytes = Base.gc_bytes()
                temp_time = get_time_ms()
                # eventually we will zip multiple concurrent streams for SDRs together here.
                # this shoudl be fast...
                for elts in zip(buffers[1]...)
                    for sample in elts
                        write(io, sample)
                    end
                end
                allocations[2] += Base.gc_bytes() - temp_bytes
                timers[2] = get_time_ms() - temp_time
            end
            if have_slack && timer_display
                temp_bytes = Base.gc_bytes()
                temp_time = get_time_ms()
                begin
                    if get_time_ms() - last_timeoutput > 1000
                        # some hackery to not allocate on the Julia GC so we use libc printf
                        @ccall printf("Read Stream: %ld allocations: %ld \n"::Cstring, timers[1]::Int, allocations[1]::Int)::Cint
                        @ccall printf("Write File: %ld allocations: %ld \n"::Cstring, timers[2]::Int, allocations[2]::Int)::Cint
                        @ccall printf("Telemetry: %ld allocations: %ld \n"::Cstring, timers[3]::Int, allocations[3]::Int)::Cint
                        @ccall _flushlbf()::Cvoid
                        last_timeoutput = get_time_ms()
                    end
                end
                allocations[3] += Base.gc_bytes() - temp_bytes
                timers[3] = get_time_ms() - temp_time
            end
            begin
                have_slack && GC.gc(false)
            end
        end
    finally
        SoapySDR.deactivate!(rxStream)
        close(io)
    end
end


end
