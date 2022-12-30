using SoapySDRRecorder
using Test
using JET
using Aqua

@testset "SoapySDRRecorder.jl" begin
    # Write your tests here.
end


Aqua.test_all(SoapySDRRecorder;
    ambiguities=false)
JET.report_package(SoapySDRRecorder)