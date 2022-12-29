# SoapySDRRecorder

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://sjkelly.github.io/SoapySDRRecorder.jl/stable)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://sjkelly.github.io/SoapySDRRecorder.jl/dev)
[![Build Status](https://github.com/sjkelly/SoapySDRRecorder.jl/workflows/CI/badge.svg)](https://github.com/sjkelly/SoapySDRRecorder.jl/actions)


This is a simple data recorder for Software Defined Radios using the SoapySDR hardware
abstraction layer. It supports the following features:

- Synchronized Multi-SDR recording
- Throughput measurements
- No allocation on the Julia GC while streaming
- Streaming compression using GZip.jl
