try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

opts = dict(name="manatee",
            version="0.0.22",
            maintainer="Quentin CAUDRON",
            maintainer_email="quentincaudron@gmail.com",
            description="manatee : a friendly wrapper around PySpark DataFrames",
            url="http://qcaudron.github.io/manatee",
            download_url="https://github.com/qcaudron/manatee",
            license="MIT",
            author="Quentin CAUDRON",
            author_email="quentincaudron@gmail.com",
            packages=["manatee"],
            )


if __name__ == '__main__':
    setup(**opts)
