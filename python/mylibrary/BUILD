python_requirements()

python_library(
  name = 'Mylibrary',
  sources = rglobs('*.py'),
  dependencies = [
    ':requests',
    ':pika',
    ':boto3',
  ],
  provides = setup_py(
    name = 'Mylibrary',
    version = '0.1.0',
    description = 'Python library.',
    zip_safe = True
  )
)
