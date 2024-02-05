from setuptools import setup, find_packages

def read_recursive_requirements(path):
    with open(path) as f:
        for line in f:
            if line.startswith('-r'):
                yield from read_recursive_requirements(line[3:].strip())
            else:
                yield line.strip()

setup(
    name='tg',
    version='0.0.1',
    description='Tools for managing a pool of Telegram accounts',
    author='Alexey Leshchenko',
    author_email='leshchenko@email.com',
    packages=find_packages(),
    install_requires=["pyrogram", "supabase", "icontract", "cloudpickle"],
    extras_require={"full": ["pandas"]},
    include_package_data=True,
)
