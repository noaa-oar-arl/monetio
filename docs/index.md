---
type: explanation
category: explanation
tags: [project, installation, platforms]
---

# Model and ObservatioN Evaluation Toolkit (MONET) Input Output (IO)

<p align="center">
  <img src="assets/MONETIO-logo.png" alt="MONETIO logo" width="220">
</p>

**MONETIO** is an open source project and Python package that aims to create a
common platform for atmospheric composition data for weather and
air quality models.

MONETIO officially supports Linux and macOS. Windows is not officially
supported or CI-tested. See the [installation guide](installing.md) for details.

MONET was developed to evaluate the Community Multiscale Air Quality Model (CMAQ)
for the NOAA National Air Quality Forecast Capability (NAQFC modeling system.
After MONET version 2.1.4, MONETIO was broken off from MONET to be its own dedicated repository [^monetio-split].
MONETIO is built to work in unison with MONET. For more information on MONET please refer to
<https://monet-arl.readthedocs.io>.

Our goal is to provide easy tools to retrieve and read atmospheric composition data in
order to speed scientific research. Currently, MONETIO is able to process
several models and observations related to air composition and meteorology.

If you use MONETIO please reference the package.

## Reference

Baker, Barry; Pan, Li. 2017. “Overview of the Model and Observation
Evaluation Toolkit (MONET) Version 1.0 for Evaluating Atmospheric
Transport Models.” Atmosphere 8, no. 11: 210

## What's New

MONETIO v0.2.7 has been released. MONETIO provides a consistent interface for reading and processing various atmospheric composition datasets through a unified reader architecture.

### Supported Datasets

**Supported Models**

- [HYSPLIT](https://www.ready.noaa.gov/HYSPLIT.php/)
- [CMAQ](https://www.epa.gov/cmaq/)
- [CAMx](https://www.camx.com/about/)
- [FV3-CHEM (UFS-Chem)](https://ufscommunity.org/)
- [WRF-CHEM](https://truenorth.eas.gatech. Georgia.edu/research/wrf-chem/)
- [CHIMERE](https://www.lmd.polytechnique.fr/chimere/)
- [RAQMS](https://raqms-ops.ssec.wisc.edu/)

**Supported Observations**

- [AirNow](https://www.airnow.gov/)
- [AQS](https://www.epa.gov/aqs/)
- [AERONET](https://aeronet.gsfc.nasa.gov/)
- [SKYNET](https://www.skynet-isdc.org/)
- [OpenAQ](https://openaq.org/)
- [NADP](https://nadp.slh.wisc.edu/)
- [CRN](https://www.ncei.noaa.gov/products/land-based-station/us-climate-reference-network)
- [TOLNet](https://www-air.larc.nasa.gov/missions/TOLNet/)
- [CEMS](https://www.epa.gov/emc/emc-continuous-emission-monitoring-systems/)
- [IMPROVE](http://vista.cira.colostate.edu/Improve/)
- [ISH (Integrated Surface Database)](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database)

### Get in Touch

- Ask questions, suggest features or view source code [on GitHub](https://github.com/noaa-oar-arl/monetio).

## Footnotes

[^monetio-split]:
    The last commit of [MONET v2.1.5](https://github.com/noaa-oar-arl/monet/releases/tag/v2.1.5)
    merged [PR#77](https://github.com/noaa-oar-arl/monet/pull/77), which brought the branch testing the split MONET and MONETIO packages into the
    primary branch of the repository.
    The first official split GitHub releases with were [MONET v2.2.0](https://github.com/noaa-oar-arl/monet/releases/tag/v2.2.0)
    and [MONETIO v0.1](https://github.com/noaa-oar-arl/monetio/releases/tag/v0.1) (Mar 2020).
