#!/usr/bin/env bash

# Update version number
cd manatee
python update_version.py
cd ..

# Build the docs
cd docs
make clean
make html
cd ..

# Xommit and push
git add -A
git commit -m "Building docs."
git push origin master

# Switch branches and pull the data we want
git checkout gh-pages
rm -rf *
touch .nojekyll
git checkout master docs/_build/html
mv docs/_build/html/* .
rm -rf docs
git add -A
git commit -m "Publishing docs."
git push origin gh-pages

# Switch back
git checkout master



# Added : release to PyPI
# python setup.py sdist bdist_wheel upload

# from http://www.willmcginnis.com/2016/02/29/automating-documentation-workflow-with-sphinx-and-github-pages/
