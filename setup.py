from setuptools import setup, find_packages
import os

setup(
    name="nerd-mega-compute",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
        "python-dotenv>=0.15.0",
    ],
    author="Adam",
    author_email="your.email@example.com",
    description="Run compute-intensive Python functions in the cloud",
    long_description=open("README.md").read() if os.path.exists("README.md") else "",
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/nerd-mega-compute",
    python_requires=">=3.6",
)