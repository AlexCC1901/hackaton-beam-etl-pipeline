import setuptools

setuptools.setup(
    name="beam-etl-pipeline",
    version="1.0.0",
    packages=setuptools.find_packages(),
    install_requires=[
        "apache-beam[gcp]==2.71.0",
        "google-cloud-aiplatform==1.42.1",
    ],
)
