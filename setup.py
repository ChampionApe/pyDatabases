import setuptools

with open("README.md", "r") as file:
  long_description = file.read()

setuptools.setup(
  name="pyDatabases",
  version="0.0.3",
  author="Rasmus K. Skjødt Berg",
  author_email="rasmus.kehlet.berg@econ.ku.dk",
  description="Small collection of database classes based primarily pandas, secondarily scipy and GAMS.",
  long_description=long_description,
  long_description_content_type="text/markdown",
  url="https://github.com/ChampionApe/pyDatabases",
  packages=setuptools.find_packages(),
  classifiers=[
    "Programming Language :: Python :: 3",
    "License :: OSI Approved :: MIT License",
  ],
  python_requires='>=3.8',
  install_requires=["pandas", "scipy","openpyxl"],
)