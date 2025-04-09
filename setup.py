from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="nerd-mega-compute",
    version="0.1.9",    author="Adam P. Wright",
    author_email="adampwright000@gmail.com",
    description="Run Python functions on powerful cloud servers with a simple decorator",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/adampwright/nerd-mega-compute",
    project_urls={
        "Bug Tracker": "https://github.com/adampwright/nerd-mega-compute/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(),
    python_requires=">=3.6",
    install_requires=[
        "requests",
        "python-dotenv",
    ],
)