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

    LoginResult login(LoginRequest request, SecretBuffer password);

    default FileReadResult readWorkspaceFile(Path relativePath) {
        return new FileReadResult.Unavailable();
    }

    default FileWriteResult writeWorkspaceFile(Path relativePath, String content) {
        return new FileWriteResult.Unavailable();
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
}
