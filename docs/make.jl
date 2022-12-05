using SoapySDRRecorder
using Documenter

DocMeta.setdocmeta!(SoapySDRRecorder, :DocTestSetup, :(using SoapySDRRecorder); recursive=true)

makedocs(;
    modules=[SoapySDRRecorder],
    authors="Steve Kelly <kd2cca@gmail.com> and contributors",
    repo="https://github.com/sjkelly/SoapySDRRecorder.jl/blob/{commit}{path}#{line}",
    sitename="SoapySDRRecorder.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://sjkelly.github.io/SoapySDRRecorder.jl",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/sjkelly/SoapySDRRecorder.jl",
)
