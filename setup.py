import setuptools


with open("README.md") as fp:
    long_description = fp.read()

# Dependencies.
with open('requirements.txt') as f:
    tests_require = f.readlines()
install_requires = [t.strip() for t in tests_require]

setuptools.setup(
    name="property-crawler",
    version="0.0.1",
    setup_requires=['lambda_setuptools'], # this config entry does the lambda packaging
    install_requires=install_requires,
    
    description="A simple App that crawls Irish residential properties",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="Andi Palo",

    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),

    python_requires=">=3.8"
)