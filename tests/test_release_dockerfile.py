from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "Dockerfile"


def test_release_dockerfile_pins_its_parser_and_base_image() -> None:
    document = DOCKERFILE.read_text(encoding="utf-8")

    assert document.startswith(
        "# syntax=docker/dockerfile:1.7@sha256:"
        "a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e\n"
    )
    assert (
        "ARG UBUNTU_IMAGE=ubuntu:26.04@sha256:"
        "678c6550cc43645e08669028bc177f50be4e7c5b8cca677067b1914d4afc7a03"
        in document
    )
    assert document.count("FROM ${UBUNTU_IMAGE}") == 3


def test_release_dockerfile_keeps_build_tools_out_of_runtime() -> None:
    document = DOCKERFILE.read_text(encoding="utf-8")
    runtime = document.split("FROM ${UBUNTU_IMAGE} AS runtime", maxsplit=1)[1]

    assert "pip uninstall --yes pip setuptools wheel" in document
    assert "pip install --only-binary=:all:" in document
    assert "COPY --from=build --chown=10001:10001 /opt/venv /opt/venv" in runtime
    assert "git" not in runtime
    assert "pip install" not in runtime
    assert "setuptools" not in runtime


def test_release_dockerfile_runs_as_the_dedicated_unprivileged_user() -> None:
    document = DOCKERFILE.read_text(encoding="utf-8")
    runtime = document.split("FROM ${UBUNTU_IMAGE} AS runtime", maxsplit=1)[1]

    assert "groupadd --gid 10001 tinvest" in runtime
    assert "useradd --uid 10001 --gid tinvest" in runtime
    assert "USER 10001:10001" in runtime
    assert runtime.index("USER 10001:10001") < runtime.index('CMD ["tinvest-api"]')
