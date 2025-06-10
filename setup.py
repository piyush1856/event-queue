from setuptools import setup, find_packages

setup(
    name="event-queue",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aiokafka>=0.8.1",
        "kafka-python>=2.0.2",
        "prometheus-client>=0.16.0",
        "python-json-logger>=2.0.7",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "isort>=5.12.0",
            "mypy>=1.0.0",
        ]
    },
    python_requires=">=3.8",
    author="Open Source Team",
    author_email="opensource@fynd.com",
    description="Async event queue implementation using Kafka",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/event-queue",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
) 