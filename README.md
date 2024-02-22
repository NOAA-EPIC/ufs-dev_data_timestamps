<h1 align="center">
UFS Weather Model Repository Develop Branch's Tracker for Populated Timestamped Datasets
</h1>

<p align="center">
    <img src="images/header.png" width="1000" height="200">
</p>

<h5 align="center">
    
[Prerequisites](#Prerequisites) • [Dataset](#Dataset) • [Quick Start](#Quick-Start)  • [Environment Setup](#Environment-Setup) • [Status](#Status)
 • [What's Included](#What's-Included)

</h5>

# About

__Introduction:__

Currently, the NOAA development teams' code managers are maintaining their datasets manually via regularly checking if a UFS timestamp dataset is being revised, committed and pushed to the UFS-WM development branch repository to maintain datasets that will only support the latest two-months of development UFS-WM code. There are times when UFS timestamp datasets are unused as they exceed the latest two-months of development UFS-WM code window and are left on-prem. While the EPIC team continues to work in parallel with the NOAA development teams' devleopment in UFS-WM code, the UFS data tracker bot will be able to support the data management of only storing datasets supporting the latest two months of development UFS-WM code within its cloud data storage via automatically tracking of the revisions that are made against the UFS timestamp datasets. 

__Purpose:__

The purpose of this script is to detect, track, and populate the revisions of the timestamp datasets made to the UFS-WM development branch. It is part of the Data Tracker Bot in Jenkins and will later be integrated with another script which will perform the 2 months window shift of datasets to maintain the NOAA development teams' code managers current practice stored and fulfill the stored data requirements.

__Capabilities:__

This script will be able to perform the following actions:
* Extract single file daily & parse
* Client makes a direct request for rt.sh from GitHub
* rt.sh is read, preprocessed & extracts the timestamps of the relevant UFS datasets which has been pushed on GitHub.
* Generates a file containing the datasets' timestamps
* Program will compare the last log file with the most recent file containing the datasets' timestamps.

__Future Capabilities:__
Will be integrated with another script, which will perform the 2 months window shift of datasets to maintain the NOAA development teams' code managers current practice and fulfill the stored data requirements.

# Table of Contents
* [Prerequisites](#Prerequisites)
* [Dataset](#Dataset)
* [Quick Start](#Quick-Start)
* [Environment Setup](#Environment-Setup) 
* [Status](#Status)
* [What's Included](#What's-Included)
* [Documentation](#Documentation)
* [References](#Reference(s))

# Prerequisites
* Python 3.9

# Dataset
* N/A

# Quick Start
* For demonstration purposes, refer to 'rt_revision_tracker_scripts_demo.ipynb'

# Environment Setup

* Install miniconda on your machine. Note: Miniconda is a smaller version of Anaconda that only includes conda along with a small set of necessary and useful packages. With Miniconda, you can install only what you need, without all the extra packages that Anaconda comes packaged with:
Download latest Miniconda (e.g. 3.9 version):

    * wget https://repo.anaconda.com/miniconda/Miniconda3-py39_4.9.2-Linux-x86_64.sh

* Check integrity downloaded file with SHA-256:

    * sha256sum Miniconda3-py39_4.9.2-Linux-x86_64.sh

Reference SHA256 hash in following link: https://docs.conda.io/en/latest/miniconda.html

* Install Miniconda in Linux:

    * bash Miniconda3-py39_4.9.2-Linux-x86_64.sh

* Next, Miniconda installer will prompt where do you want to install Miniconda. Press ENTER to accept the default install location i.e. your $HOME directory. If you don't want to install in the default location, press CTRL+C to cancel the installation or mention an alternate installation directory. If you've chosen the default location, the installer will display “PREFIX=/var/home//miniconda3” and continue the installation.

* For installation to take into effect, run the following command:

source ~/.bashrc

* Next, you will see the prefix (base) in front of your terminal/shell prompt. Indicating the conda's base environment is activated. Once you have conda installed on your machine, perform the following to create a conda environment:

    * To create a new environment (if a YAML file is not provided)

        * conda create -n [Name of your conda environment you wish to create]

(OR)

    * To ensure you are running Python 3.9:

        * conda create -n myenv Python=3.9

(OR)

* To create a new environment from an existing YAML file (if a YAML file is provided):

    * conda env create -f environment.yml

*Note: A .yml file is a text file that contains a list of dependencies, which channels a list for installing dependencies for the given conda environment. For the code to utilize the dependencies, you will need to be in the directory where the environment.yml file lives.

## Activate the new environment via:

conda activate [Name of your conda environment you wish to activate]

* Verify that the new environment was installed correctly via:

    * conda info --env

*Note:

* From this point on, must activate conda environment prior to .py script(s) or jupyter notebooks execution using the following command: conda activate
    * To deactivate a conda environment:
        * conda deactivate

## Link Home Directory to Dataset Location on RDHPCS Platform

* Unfortunately, there is no way to navigate to the "/work/" filesystem from within the Jupyter interface when working on the remote server, Orion. The best way to workaround is to create a symbolic link in your home folder that will take you to the /work/ filesystem. Run the following command from a linux terminal on Orion to create the link:

    * ln -s /work /home/[Your user account name]/work

* Now, when you navigate to the /home/[Your user account name]/work directory in Jupyter, it will take you to the /work folder. Allowing you to obtain any data residing within the /work filesystem that you have permission to access from Jupyter. This same procedure will work for any filesystem available from the root directory.

*Note: On Orion, user must sym link from their home directory to the main directory containing the datasets of interest.

## Open & Run Application on Jupyter Notebook

* Open OnDemand has a built-in file explorer and file transfer application available directly from its dashboard via:

    * Login to https://orion-ood.hpc.msstate.edu/

* In the Open OnDemand Interface, select Interactive Apps > Jupyter Notbook

## Additonal Information

To create a .yml file, execute the following commands:

* Activate the environment to export:

    * conda activate myenv

* Export your active environment to a new file:

    * conda env export > [ENVIRONMENT FILENAME].yml

# Status
[![Version badge](https://img.shields.io/badge/Python-3.9-blue.svg)](https://shields.io/)
[![Build badge](https://img.shields.io/badge/Build--gray.svg)](https://shields.io/)

# What's Included
Within the download, you will find the following directories and files:
* Demo:
    > rt_revision_tracker_scripts_demo.ipynb
* Scripts:
    > rt_revision_tracker.py
    > rt_tracker_populate.py
    > rt_tracker_reset.py
* List of Dependencies: 
    > git_env.yml

# Version:
* Draft as of 03/14/24

