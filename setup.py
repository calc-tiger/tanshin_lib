from setuptools import setup, find_packages

setup(
    name="tanshin_lib",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests",
        "pandas",
        "pdfminer.six",
        "pdfplumber",
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="A library to parse Japanese financial PDF reports (Tanshin).",
    url="https://github.com/yourusername/tanshin_lib",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)
