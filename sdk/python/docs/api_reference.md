# API Reference

.. currentmodule:: feast

The main concepts with Feast API are:
- **Client** is the interface to Feast. The client methods are used to establish
  connection to Feast in order to register entities, features and to start
  an import job to ingest feature values to Feast.
- **Importer** is used to create an import configuration for ingesting feature
  values to Feast.

## Client

.. autosummary::
   :toctree: _autosummary

   client.Client

## Feature Set

.. autosummary::
   :toctree: _autosummary
   
   feature_set.FeatureSet

## Entity

.. autosummary::
   :toctree: _autosummary
   
   entity.Entity

## Job

.. autosummary::
   :toctree: _autosummary
   
   job.Job
