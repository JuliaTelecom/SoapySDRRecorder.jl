module SoapySDRRecorder

using SoapySDR
using SigMF
using TimerOutputs


get_time_ms() = trunc(Int, time() * 1000)

function record(output_file ;direct_buffer_access = true, timer_display = true)
    devs = Devices()

    sdr = Device(devs[1])

    rx1 = sdr.rx[1]

    sampRate = 2.048e6

    rx1.sample_rate = sampRate * u"Hz"

    # Enable automatic Gain Control
    rx1.gain_mode = true

    to = TimerOutput()

    f0 = 104.1e6

    rx1.frequency = f0 * u"Hz"

    # set up a stream (complex floats)
    format = rx1.native_stream_format
    rxStream = SoapySDR.Stream(format, [rx1])

    # create a re-usable buffer for rx samples
    buffsz = rxStream.mtu
    buff = Array{format}(undef, buffsz)
    buffs = Ptr{format}[C_NULL] # pointer for direct buffer API

    last_timeoutput = get_time_ms()

    io = open(output_file, "w")

    # If there is timing slack, we can sleep a bit to run event handlers
    have_slack = true

    # Enable ther stream
    @info "streaming..."
    SoapySDR.activate!(rxStream)
    while true
        @timeit to "Reading stream" begin
            if !direct_buffer_access
                read!(rxStream, (buff,))
            else
                err, handle, flags, timeNs =
                    SoapySDR.SoapySDRDevice_acquireReadBuffer(sdr, rxStream, buffs, 0)
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
                SoapySDR.SoapySDRDevice_releaseReadBuffer(sdr, rxStream, handle)
            end
        end
        @timeit to "Write to file" begin
            write(io, buff)
        end
        if have_slack && timer_display
            @timeit to "Timer Display" begin
                if get_time_ms() - last_timeoutput > 3000
                    show(to)
                    last_timeoutput = get_time_ms()
                end
            end
        end
        @timeit to "GC" begin
            have_slack && GC.gc(false)
        end
    end
end


function record1(output_file
                ;device::Union{SoapySDR.Device,Nothing}=nothing,
                channels::Union{AbstractArray{<:SoapySDR.Channel},Nothing}=nothing,
                )
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

    num_channels = if channels === nothing
        length(device.rx)
    else
        length(channels)
    end

    channels = if channels === nothing
        device.rx
    else
        channels
    end

    # here we are reach a type instability, so we need to function barrier this part    
    format = device.rx[first(channels)].native_stream_format

    stream = SoapySDR.Stream(channels)
    mtu = stream.mtu

    # Allocate some buffers
    buffers = [ntuple(_ -> Vector(format, mtu), num_channels) for _ in 1:num_buffers]

    to = TimerOutput()

    # compute timeout

    #TODO: avoid closure construction
    SoapySDR.activate!(stream) do
        open(output_file, "w") do io
            while true
                # read a buffer
                @timeit to "SDR Read" SoapySDR.read!(rx_stream, buffers[1])
                # write the buffer to the file
                @timeit to "File Write" write(io, buffer[1])
            end
        end
    end
end

end
