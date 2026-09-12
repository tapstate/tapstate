package io.tapstate.cli;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Typed activation boundary used by the workbench without routing through command text. */
interface WorkbenchActionGateway {

    List<ContextOption> contexts();

    ContextResult selectContext(String name);

    ContextResult createContext(String name, URI server, boolean verifyTls);

    default ContextDeleteResult deleteContext(String name) {
        return new ContextDeleteResult.Unavailable();
    }

    LoginResult login(LoginRequest request, SecretBuffer password);

    default FileReadResult readWorkspaceFile(Path relativePath) {
        return new FileReadResult.Unavailable();
    }

    default FileWriteResult writeWorkspaceFile(Path relativePath, String content) {
        return new FileWriteResult.Unavailable();
    }

    default SourceCatalogResult sourceCatalog() {
        return new SourceCatalogResult.Unavailable();
    }

    default SourcePreviewResult previewSource(SourceDraft draft) {
        return new SourcePreviewResult.Unavailable();
    }

    default SourceCreateResult createSource(SourceDraft draft) {
        return new SourceCreateResult.Unavailable();
    }

    record ContextOption(String name, boolean suggested) {
        public ContextOption {
            Objects.requireNonNull(name, "name");
        }
    }

    record LoginRequest(Optional<URI> server, String username) {
        public LoginRequest {
            Objects.requireNonNull(server, "server");
            Objects.requireNonNull(username, "username");
        }
    }

    sealed interface ContextResult {
        record Ready(String contextName, boolean signedIn) implements ContextResult {
            public Ready {
                Objects.requireNonNull(contextName, "contextName");
            }
        }

        record Offline(String contextName) implements ContextResult {
            public Offline {
                Objects.requireNonNull(contextName, "contextName");
            }
        }

        record Unavailable() implements ContextResult {
        }
    }

    sealed interface LoginResult {
        record SignedIn(String principal) implements LoginResult {
            public SignedIn {
                Objects.requireNonNull(principal, "principal");
            }
        }

        record Rejected(String code) implements LoginResult {
            public Rejected {
                Objects.requireNonNull(code, "code");
            }
        }

        record Unreachable() implements LoginResult {
        }

        record Unavailable() implements LoginResult {
        }
    }

    sealed interface ContextDeleteResult {
        record Deleted(String contextName) implements ContextDeleteResult {
            public Deleted {
                Objects.requireNonNull(contextName, "contextName");
            }
        }

        record Unavailable() implements ContextDeleteResult {
        }
    }

    sealed interface FileReadResult {
        record Loaded(Path relativePath, String content) implements FileReadResult {
            public Loaded {
                Objects.requireNonNull(relativePath, "relativePath");
                Objects.requireNonNull(content, "content");
            }
        }

        record Unavailable() implements FileReadResult {
        }
    }

    sealed interface FileWriteResult {
        record Saved() implements FileWriteResult {
        }

        record Unavailable() implements FileWriteResult {
        }
    }

    record SourceConnector(String id, List<String> modes) {
        public SourceConnector {
            Objects.requireNonNull(id, "id");
            modes = List.copyOf(modes);
        }
    }

    record SourceCatalog(List<SourceConnector> connectors) {
        public SourceCatalog {
            connectors = List.copyOf(connectors);
        }
    }

    record SourceDraft(String connector, String mode, String tables, String id) {
        public SourceDraft {
            Objects.requireNonNull(connector, "connector");
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(tables, "tables");
            Objects.requireNonNull(id, "id");
        }
    }

    sealed interface SourceCatalogResult {
        record Ready(SourceCatalog catalog) implements SourceCatalogResult {
            public Ready {
                Objects.requireNonNull(catalog, "catalog");
            }
        }

        record Unavailable() implements SourceCatalogResult {
        }
    }

    sealed interface SourcePreviewResult {
        record Ready(String canonicalYaml) implements SourcePreviewResult {
            public Ready {
                Objects.requireNonNull(canonicalYaml, "canonicalYaml");
            }
        }

        record Rejected(String message) implements SourcePreviewResult {
            public Rejected {
                Objects.requireNonNull(message, "message");
            }
        }

        record Unavailable() implements SourcePreviewResult {
        }
    }

    sealed interface SourceCreateResult {
        record Created(Path relativePath, String canonicalYaml) implements SourceCreateResult {
            public Created {
                Objects.requireNonNull(relativePath, "relativePath");
                Objects.requireNonNull(canonicalYaml, "canonicalYaml");
            }
        }

        record Exists(Path relativePath) implements SourceCreateResult {
            public Exists {
                Objects.requireNonNull(relativePath, "relativePath");
            }
        }

        record Rejected(String message) implements SourceCreateResult {
            public Rejected {
                Objects.requireNonNull(message, "message");
            }
        }

        record Unavailable() implements SourceCreateResult {
        }
    }
}
