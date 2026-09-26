"""Embedding backends and embedding profiles.

Two backends:

``sentence-transformers``  the real one. A local, CPU-capable model (default
    ``BAAI/bge-small-en-v1.5`` pinned to a Hugging Face commit: 384 dimensions,
    512-token input limit, L2-normalised output, cosine similarity). The model is
    loaded with ``local_files_only=True``: it is never downloaded on import, on
    load or during tests. ``datasources-similarity download-model`` fetches it.
``hashing``  deterministic bag-of-words hashing for tests and offline demos. It
    captures word overlap only, and it gets its own profile, so its vectors can
    never be compared with real model vectors.

An *embedding profile* (the existing ``embedding_profile`` table) identifies one
embedding space: model provider/name/version, dimensions, distance metric, text
preparation version and every setting that changes the vectors. Its
``config_hash`` is the identity, so a model or preprocessing change produces a
new profile and new, versioned work instead of mixing spaces whose dimensions
merely happen to match.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import logging
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from ..ingest.settings import ConfigurationError
from .settings import TEXT_PREP_VERSION, SimilaritySettings

LOGGER = logging.getLogger(__name__)

_HASH_TOKEN = re.compile(r"[a-z0-9]+(?:[.,][0-9]+)*%?")
_HASH_STOPWORDS = frozenset(
    "a an and are as at be by for from has have in is it its of on or that the this to was were will with".split())
_ALTERNATIVE_WEIGHTS = ["onnx/*", "openvino/*", "*.onnx", "*.h5", "*.msgpack", "*.ot", "*.tflite", "coreml/*"]


class EmbeddingError(RuntimeError):
    """Embedding failed at run time. Retryable failures leave analysis pending."""

    def __init__(self, message: str, *, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable


class EmbeddingConfigurationError(ConfigurationError):
    """The configured embedding backend cannot be used (package or model missing)."""


@dataclass(frozen=True)
class EmbeddingIdentity:
    model_provider: str
    model_name: str
    model_version: str
    dimensions: int
    distance_metric: str
    max_input_tokens: int
    encoding: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProfileSpec:
    """Everything that defines one embedding space, as stored in ``embedding_profile``."""

    identity: EmbeddingIdentity
    chunk_tokens: int
    chunk_overlap_tokens: int
    chunking_version: str = TEXT_PREP_VERSION
    runtime: dict[str, Any] = field(default_factory=dict)  # recorded, not part of the identity

    def identity_payload(self) -> dict[str, Any]:
        ident = self.identity
        return {
            "model_provider": ident.model_provider,
            "model_name": ident.model_name,
            "model_version": ident.model_version,
            "dimensions": ident.dimensions,
            "distance_metric": ident.distance_metric,
            "chunking_version": self.chunking_version,
            "encoding": ident.encoding,
            "max_input_tokens": ident.max_input_tokens,
            "text_units": {"chunk_tokens": self.chunk_tokens, "chunk_overlap_tokens": self.chunk_overlap_tokens},
        }

    @property
    def config_hash(self) -> str:
        canonical = json.dumps(self.identity_payload(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def configuration(self) -> dict[str, Any]:
        payload = self.identity_payload()
        return {"encoding": payload["encoding"], "max_input_tokens": payload["max_input_tokens"],
                "text_units": payload["text_units"], "runtime_at_creation": self.runtime}


class EmbeddingBackend(Protocol):
    def identity(self) -> EmbeddingIdentity: ...

    def count_tokens(self, text: str) -> int: ...

    def embed(self, texts: list[str]) -> list[list[float]]: ...

    def runtime_info(self) -> dict[str, Any]: ...


# ----------------------------------------------------------------------------- vectors
def validate_vectors(vectors: list[list[float]], dimensions: int) -> list[list[float]]:
    """Reject wrong dimensions, non-finite values and zero vectors (cosine is undefined for them)."""
    checked: list[list[float]] = []
    for index, vector in enumerate(vectors):
        values = [float(x) for x in vector]
        if len(values) != dimensions:
            raise EmbeddingError(f"vector {index} has {len(values)} dimensions, profile expects {dimensions}",
                                 retryable=False)
        if not all(math.isfinite(x) for x in values):
            raise EmbeddingError(f"vector {index} contains non-finite values", retryable=False)
        if math.sqrt(sum(x * x for x in values)) == 0.0:
            raise EmbeddingError(f"vector {index} is a zero vector", retryable=False)
        checked.append(values)
    return checked


def vector_literal(vector: list[float]) -> str:
    """pgvector text form. Nine significant digits round-trip float32 exactly."""
    return "[" + ",".join(f"{x:.9g}" for x in vector) + "]"


def parse_vector(text: str) -> list[float]:
    return [float(x) for x in text.strip().strip("[]").split(",") if x]


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b))
    return dot / norm if norm else 0.0


# ----------------------------------------------------------------------------- hashing
class HashingEmbeddingBackend:
    """Deterministic bag-of-words hashing (tests and offline demos; not semantic)."""

    def __init__(self, dimensions: int = 256):
        self.dimensions = dimensions

    def identity(self) -> EmbeddingIdentity:
        return EmbeddingIdentity("datasources", "hashing-bow", "1", self.dimensions, "cosine", 100_000,
                                 {"normalize": True, "features": "word-unigrams+bigrams", "stopwords": "en-basic"})

    def count_tokens(self, text: str) -> int:
        return len(re.findall(r"\w+|[^\w\s]", text)) + 2

    def runtime_info(self) -> dict[str, Any]:
        return {"backend": "hashing"}

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_one(text) for text in texts]

    def _embed_one(self, text: str) -> list[float]:
        words = [w for w in _HASH_TOKEN.findall(text.lower()) if w not in _HASH_STOPWORDS]
        features = words + [f"{a} {b}" for a, b in zip(words, words[1:])]
        vector = [0.0] * self.dimensions
        for feature in features:
            digest = hashlib.sha256(feature.encode("utf-8")).digest()
            index = int.from_bytes(digest[:4], "big") % self.dimensions
            vector[index] += 1.0 if digest[4] & 1 else -1.0
        norm = math.sqrt(sum(x * x for x in vector))
        if norm == 0.0:
            raise EmbeddingError("text has no hashable words", retryable=False)
        return [x / norm for x in vector]


# ----------------------------------------------------------------------------- sentence-transformers
def _quiet_hf() -> None:
    try:
        from huggingface_hub.utils import disable_progress_bars

        disable_progress_bars()
    except Exception:  # pragma: no cover - cosmetic only
        pass
    try:
        from transformers.utils import logging as hf_logging

        hf_logging.disable_progress_bar()
        hf_logging.set_verbosity_error()
    except Exception:  # pragma: no cover - cosmetic only
        pass


def _install_hint() -> str:
    return "install it with `uv sync --locked --extra local-embeddings`"


class SentenceTransformerBackend:
    """Local sentence-transformers model on CPU, loaded from the local cache only."""

    def __init__(self, model_name: str, revision: str, *, device: str = "cpu", batch_size: int = 16,
                 cache_dir: str | None = None):
        self.model_name = model_name
        self.revision = revision
        self.device = device
        self.batch_size = batch_size
        self.cache_dir = cache_dir
        self._model = None
        self._identity: EmbeddingIdentity | None = None

    def load(self) -> "SentenceTransformerBackend":
        if self._model is not None:
            return self
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as error:
            raise EmbeddingConfigurationError(
                f"SIMILARITY_EMBEDDING_BACKEND=sentence-transformers needs the sentence-transformers package; "
                f"{_install_hint()}") from error
        _quiet_hf()
        try:
            model = SentenceTransformer(self.model_name, revision=self.revision, device=self.device,
                                        cache_folder=self.cache_dir, local_files_only=True)
        except Exception as error:
            raise EmbeddingConfigurationError(
                f"embedding model {self.model_name}@{self.revision} is not available locally ({error}). "
                "Download it explicitly with `uv run datasources-similarity download-model`.") from error
        dimensions = model.get_embedding_dimension()
        max_tokens = int(model.max_seq_length or 0)
        if not dimensions or max_tokens <= 0:
            raise EmbeddingConfigurationError(f"model {self.model_name} reports no dimension / sequence limit")
        # Pooling and module layout are fixed by the pinned model revision, so only
        # the encoding choices made here belong to the profile identity.
        self._modules = [type(module).__name__ for module in model]
        self._identity = EmbeddingIdentity(
            model_provider="sentence-transformers",
            model_name=self.model_name,
            model_version=self._resolved_revision(),
            dimensions=int(dimensions),
            distance_metric="cosine",
            max_input_tokens=max_tokens,
            encoding={"normalize_embeddings": True, "query_instruction": None, "truncation": "never"},
        )
        self._model = model
        return self

    def _resolved_revision(self) -> str:
        if re.fullmatch(r"[0-9a-f]{40}", self.revision or ""):
            return self.revision
        try:  # a branch or tag name: record the commit actually in the local cache
            from huggingface_hub import snapshot_download

            path = snapshot_download(self.model_name, revision=self.revision, cache_dir=self.cache_dir,
                                     local_files_only=True)
            return Path(path).name
        except Exception:
            return self.revision

    @property
    def model(self):
        return self.load()._model

    def identity(self) -> EmbeddingIdentity:
        self.load()
        assert self._identity is not None
        return self._identity

    def count_tokens(self, text: str) -> int:
        encoded = self.model.tokenizer(text, add_special_tokens=True, truncation=False, verbose=False)
        return len(encoded["input_ids"])

    def runtime_info(self) -> dict[str, Any]:
        info: dict[str, Any] = {"backend": "sentence-transformers", "device": self.device,
                                "modules": getattr(self, "_modules", None)}
        for package in ("sentence-transformers", "transformers", "torch"):
            try:
                info[package] = importlib.metadata.version(package)
            except importlib.metadata.PackageNotFoundError:
                info[package] = None
        return info

    def embed(self, texts: list[str]) -> list[list[float]]:
        identity = self.identity()
        too_long = [i for i, text in enumerate(texts) if self.count_tokens(text) > identity.max_input_tokens]
        if too_long:  # the text-unit builder should make this impossible; never truncate silently
            raise EmbeddingError(f"inputs {too_long} exceed the model limit of {identity.max_input_tokens} tokens",
                                 retryable=False)
        try:
            array = self.model.encode(texts, batch_size=self.batch_size, normalize_embeddings=True,
                                      convert_to_numpy=True, show_progress_bar=False)
        except Exception as error:  # device / runtime failures
            raise EmbeddingError(f"embedding model failed: {error}") from error
        return [row.tolist() for row in array]


# ----------------------------------------------------------------------------- factory
def build_embedding_backend(settings: SimilaritySettings):
    if settings.embedding_backend == "hashing":
        return HashingEmbeddingBackend(settings.hashing_dimensions)
    return SentenceTransformerBackend(settings.embedding_model, settings.embedding_revision,
                                      device=settings.embedding_device, batch_size=settings.embedding_batch_size,
                                      cache_dir=settings.embedding_cache_dir)


def profile_spec(backend, settings: SimilaritySettings) -> ProfileSpec:
    return ProfileSpec(identity=backend.identity(), chunk_tokens=settings.chunk_tokens,
                       chunk_overlap_tokens=settings.chunk_overlap_tokens, runtime=backend.runtime_info())


def download_model(settings: SimilaritySettings) -> Path:
    """Explicitly download the configured model into the Hugging Face cache (network)."""
    if settings.embedding_backend != "sentence-transformers":
        raise ConfigurationError("download-model only applies to SIMILARITY_EMBEDDING_BACKEND=sentence-transformers")
    try:
        from huggingface_hub import HfApi, snapshot_download
    except ImportError as error:
        raise EmbeddingConfigurationError(f"huggingface_hub is missing; {_install_hint()}") from error
    files = HfApi().list_repo_files(settings.embedding_model, revision=settings.embedding_revision)
    ignore = list(_ALTERNATIVE_WEIGHTS)
    if any(name.endswith(".safetensors") for name in files):
        ignore.append("*.bin")  # identical weights; sentence-transformers prefers safetensors
    path = snapshot_download(settings.embedding_model, revision=settings.embedding_revision,
                             cache_dir=settings.embedding_cache_dir, ignore_patterns=ignore)
    return Path(path)
