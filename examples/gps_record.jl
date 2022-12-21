using SoapyRTLSDR_jll, SoapySDR, SoapySDRRecorder, Unitful

# Channel confuguration callback
function configuration(dev, chans)

    dev[SoapySDR.Setting("biastee")] = "true"

    for rx in chans
        rx.sample_rate = 3u"MHz"
        rx.bandwidth = 3u"MHz"
        rx.frequency = 1_575_420_000u"Hz"
        rx.gain_mode = true
        @show rx
    end

end

function telemetry_callback(dev, chans)
    # this is terribly low level, but for some reason the highlevel API
    # segfaults julia, so we keep it all in C
    #@ccall printf("%s\n"::Cstring, SoapySDR.SoapySDRDevice_readSetting(dev.ptr, "DMA_BUFFERS")::Ptr{Cstring})::Cint
end

SoapySDRRecorder.record("test", device=Device(SoapySDR.Devices()[1]), channel_configuration=configuration,
    telemetry_callback=telemetry_callback, compress=false, compression_level=5,
    stream_type=Complex{Int8}) # timeout=100000)

