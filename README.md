# PPF Market Research Data Pipeline

This project builds a data pipeline for collecting, validating, and enriching information on paint protection film (PPF), tint, wrap, and auto detailing shops across the United States.

The goal of this project is to turn raw business listing data into a structured dataset that can support market research, competitive analysis, and downstream data analysis.

## Overview

This project combines multiple sources and processing steps to build a more complete business dataset. The pipeline includes:

- business discovery through Yelp
- cross-checking and validation through Google Places
- website-based enrichment for additional business details
- structured output for analysis and reporting

The final output is designed to support practical business research, including identifying shop locations, contact information, online presence, and brand usage patterns.

## Repository Scope

This repository includes a selected set of representative project files rather than every development version. During development, I created multiple iterations as I expanded the project from smaller local experiments to a broader nationwide pipeline. I selected the clearest and most useful versions here so the repository stays organized and easy to review.

The files included in this repository reflect the main project workflow and show how the pipeline evolved from a city-level version to a larger US-wide version.

## Project Files

- `ppf_us_50_states_v3_verified.py`  
  Main nationwide pipeline. This script discovers and enriches PPF and related automotive service businesses across the United States using Yelp and Google Places data.

- `scrape_ppf_atlanta_v2.py`  
  Earlier regional version focused on the Atlanta market. This version was part of the development process and helped test and refine the pipeline logic before scaling to a broader geographic scope.

- `PPF_US_full_v3.xlsx`  
  Example output dataset generated from the pipeline.

## Key Features

- multi-source business discovery
- Yelp API integration
- Google Places verification and enrichment
- website scraping for additional details
- extraction of contact information, email, and social links
- identification of PPF-related brands mentioned on business websites
- structured Excel output for reporting and analysis

## Example Data Fields

The dataset may include fields such as:

- Shop Name
- Address
- City
- State
- ZIP
- Contact Number
- Website
- Email
- Instagram
- Facebook
- Google Rating
- Google Reviews
- Yelp Rating
- Yelp Reviews
- Main PPF Brands Used
- Latitude / Longitude
- Last Checked (UTC)

## How to Run

### 1. Set API keys

Before running the scripts, set your API keys as environment variables:

```bash
export YELP_API_KEY=your_key_here
export GOOGLE_API_KEY=your_key_here
