try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

opts = dict(name="manatee",
            maintainer="Quentin CAUDRON",
            maintainer_email="quentincaudron@gmail.com",
            description="manatee : a friendly wrapper around PySpark DataFrames",
            url="",
            download_url="",
            license="MIT",
            author="Quentin CAUDRON",
            author_email="Quentin CAUDRON",
            version="0.0.1",
            packages=["manatee"]
            )


if __name__ == '__main__':
    setup(**opts)
