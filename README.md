# BAS Download Toolbox

![GitHub issues](https://img.shields.io/github/issues/environmental-forecasting/download-toolbox?style=plastic)
![GitHub closed issues](https://img.shields.io/github/issues-closed/environmental-forecasting/download-toolbox?style=plastic)
![GitHub](https://img.shields.io/github/license/environmental-forecasting/download-toolbox)
![GitHub forks](https://img.shields.io/github/forks/environmental-forecasting/download-toolbox?style=social)
![GitHub forks](https://img.shields.io/github/stars/environmental-forecasting/download-toolbox?style=social)

This is a python library providing CLI operations allowing users to download 
common environmental datasets for use in data pipelines. We use this within our 
optimisation and machine learning pipelines within BAS and it should be flexible 
enough to adapt to many different use cases.

Contact `digitalinnovation <at> bas <dot> ac <dot> uk` if you want further information.

## Table of contents

* [Installation](#installation)
* [Implementation](#implementation)
* [Basic Principles](#basic-principles)
* [Limitations](#limitations)
* [Contributing](#contributing)
* [Credits](#credits)
* [License](#license)

## Installation

`pip install download-toolbox`

Please refer to [the contribution guidelines for more information][1].

## Implementation

When installed, the library will provide a series of CLI commands. Please use 
the `--help` switch for more initial information, or the documentation. 

### Basic principles

The library sets up downloaders that will go through the following steps, 
for a variety of different data sources:

1. Set up a data store or if it exists, read the provenance config
2. Naively optimise the requested download
3. Download from the source in parallel
4. Transform the dataset into convenient to use files, ready for processing

That last step is important, as it might result in a different dataset to that which comes 
from source. The tool is intended to record this in the provenenace configuration, which is 
why it might exist in step (1), so that new data downloaded is consistent with what's 
there - as well as the differences from the source data recorded for consistency (you 
should not be able to screw up existing datasets), posterity and reproducibility. 

## Limitations

There are some major limitations to this as a general purpose tool, these will 
hopefully be dealt with in time! They likely don't have issues related, yet.

* Works only for hemisphere level downloading - north or south. The overhaul for this intends to ensure that identifiers are used so that someone can specify "north" or "south" but equally specify "Norway" or "The Shops" and then provide a geolocation that would identify the dataset within the filesystem.

**This is currently very heavy development functionality, but the following downloaders should work**: 

* download_amsr2
* download_cmip
* download_era5
* download_osisaf

Other stubs might not work, but there is a chance I'll forget to update these docs!

## Contributing 

Please refer to [the contribution guidelines for more information][1].

## Credits

<a href="https://github.com/environmental-forecasting/download-toolbox/graphs/contributors"><img src="https://contrib.rocks/image?repo=environmental-forecasting/download-toolbox" /></a>

## License

This is licensed using the [MIT License][2].

[1]: https://github.com/environmental-forecasting/download-toolbox/CONTRIBUTING.md
[2]: https://github.com/environmental-forecasting/download-toolbox/LICENSE