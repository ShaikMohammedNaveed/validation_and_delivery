from setuptools import find_packages, setup

setup(
    name="data_ingest",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    # Add more metadata as needed
    author="Kunal Goyal",
    description="This module takes input as a Gdrive Folder as link and outputs Colab files.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    # url='https://github.com/yourusername/my_package',
    # install_requires=[
    #     # List your project dependencies here
    #     # e.g., 'requests>=2.23.0'
    # ],
    # More configurations such as `scripts`, `entry_points`, etc. can be added if needed.
)
