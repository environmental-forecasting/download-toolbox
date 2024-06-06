# Welcome to Download Toolbox's documentation!

## Table of contents

```{eval-rst}
.. toctree::
      :maxdepth: 2
      :caption: Contents:

      readme
      installation
      usage
      modules
      contributing
      authors
      history
```

## Description

### Overview

The structure of download-toolbox is centered around three primary objects contained within the `download_toobox.base` module.

* DataCollection - the base structure for data collections within the project
* DataSet - a dataset is an implementation of the base collection, adding characteristics.
* Downloader - a class implementing downloading functionality against the data collection

The intention is that other toolboxes will take DataSets / DataCollections that can be backed by either file, object or other storages in the future. The downloader then operates against them, with additional toolboxes using these and extending them. All of these objects can be extended to automatically track and record metadata and configurations for the collections.

This simple methodology makes it easy(-ier) for new collections and activities to be extended.

Previously these elements resided in `icenet` where much of the implementations were increasingly diverging, so this rationalisation improves the consistency across different data sources.

### Commands

## Indices and tables

```{eval-rst}
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
```
