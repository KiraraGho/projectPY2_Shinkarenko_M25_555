install:
	poetry install

project:
	poetry run project

build:
	poetry build

publish:
	poetry publish --dry-run

package-install:
	pipx install dist/*.whl --force

lint:
	poetry run ruff check .

