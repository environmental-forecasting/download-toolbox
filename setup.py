from setuptools import setup, find_packages

import download_toolbox

"""Setup module for download_toolbox
"""


def get_content(filename):
    with open(filename, "r") as fh:
        return fh.read()


setup(
    name=download_toolbox.__name__,
    version=download_toolbox.__version__,
    author=download_toolbox.__author__,
    author_email=download_toolbox.__email__,
    description="Library for downloading and preprocessing various climate and observation datas",
    long_description="""{}\n---\n""".
                     format(get_content("README.md"),
                            get_content("HISTORY.rst")),
    long_description_content_type="text/markdown",
    url="https://github.com/antarctica/download-toolbox",
    packages=find_packages(),
    keywords="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        ],
    entry_points={
        "console_scripts": [
            "download_cmip = download_toolbox.data.esgf:main",
            "download_era5 = download_toolbox.data.cds:main",
            "download_oras5 = download_toolbox.data.cmems:main",
            "download_hres = download_toolbox.data.mars:hres_main",
            "download_seas = download_toolbox.data.mars:seas_main",
            "download_osisaf = download_toolbox.data.osisaf:main",
            "download_amsr2 = download_toolbox.data.amsr:main",
        ],},
    python_requires='>=3.8, <4',
    install_requires=get_content("requirements.txt"),
    include_package_data=True,
    extras_require={
        "dev": get_content("requirements_dev.txt"),
        "docs": get_content("docs/requirements.txt"),
        },
    test_suite='tests',
    tests_require=['pytest>=3'],
    zip_safe=False,
)
