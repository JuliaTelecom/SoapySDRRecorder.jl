using SoapySDR, SoapySDRRecorder, Unitful

# Channel confuguration callback
function configuration(dev, chans)

    #dev[SoapySDR.Setting("biastee")] = "true"

    for rx in chans
        rx.sample_rate = 2u"MHz"
        rx.frequency = 1_575_420_000u"Hz"
        @show rx
    end

end

SoapySDRRecorder.record("test", device=Device(SoapySDR.Devices()[1]), channel_configuration=configuration)

