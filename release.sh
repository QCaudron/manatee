#!/usr/bin/env bash

# build the docs
cd docs
make clean
make html
cd ..

# commit and push
git add -A
git commit -m "Building docs."
git push origin master

# switch branches and pull the data we want
git checkout gh-pages
rm -rf *
touch .nojekyll
git checkout master docs/_build/html
mv docs/_build/html/* .
rm -rf docs
git add -A
git commit -m "Publishing docs."
git push origin gh-pages

# switch back
git checkout master



# Added : release to PyPI
# python setup.py sdist bdist_wheel upload

# from http://www.willmcginnis.com/2016/02/29/automating-documentation-workflow-with-sphinx-and-github-pages/