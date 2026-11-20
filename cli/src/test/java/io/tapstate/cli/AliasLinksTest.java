package io.tapstate.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The optional {@code tap} shortcut, managed after installation. {@code tapstate} is the only real
 * command; this exists so a user who declined the shortcut at install time - or whose machine had the
 * name taken - can change their mind without reinstalling.
 *
 * <p>Every case here is about a link rather than a copy. A copy would be correct the day it is made
 * and wrong at the next upgrade, which is exactly the failure a shortcut is least likely to have
 * anyone watching for.
 */
class AliasLinksTest {

    @Test
    void installLinksTheShortcutToTheRealCommand(@TempDir Path binDir) throws IOException {
        Files.createFile(binDir.resolve("tapstate"));

        AliasLinks.install(binDir);

        Path alias = binDir.resolve("tap");
        assertThat(Files.isSymbolicLink(alias)).isTrue();
        assertThat(alias.toRealPath()).isEqualTo(binDir.resolve("tapstate").toRealPath());
    }

    @Test
    void installIsRepeatableRatherThanRefusingItsOwnLink(@TempDir Path binDir) throws IOException {
        // Re-running is how a user re-points the shortcut after an upgrade, so its own link is not a
        // conflict. Only a `tap` that belongs to something else is.
        Files.createFile(binDir.resolve("tapstate"));
        AliasLinks.install(binDir);

        AliasLinks.install(binDir);

        assertThat(binDir.resolve("tap").toRealPath()).isEqualTo(binDir.resolve("tapstate").toRealPath());
    }

    @Test
    void uninstallRemovesOnlyTheShortcut(@TempDir Path binDir) throws IOException {
        Files.createFile(binDir.resolve("tapstate"));
        AliasLinks.install(binDir);

        AliasLinks.uninstall(binDir);

        assertThat(Files.exists(binDir.resolve("tap"), java.nio.file.LinkOption.NOFOLLOW_LINKS)).isFalse();
        assertThat(Files.exists(binDir.resolve("tapstate"))).isTrue();
    }

    @Test
    void aNameThatBelongsToSomethingElseIsRefusedAndLeftUntouched(@TempDir Path binDir) throws IOException {
        // node-tap ships a `tap`. Overwriting it would remove a working command from the user's machine
        // to install a convenience they can live without, so the refusal is the whole point - and the
        // assertion has to prove the file survived, not merely that a throw happened.
        Files.createFile(binDir.resolve("tapstate"));
        Path foreign = binDir.resolve("tap");
        Files.writeString(foreign, "#!/bin/sh\necho not tapstate\n");

        assertThatThrownBy(() -> AliasLinks.install(binDir))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code().code())
                        .isEqualTo("TS0330006"));

        assertThat(Files.isSymbolicLink(foreign)).isFalse();
        assertThat(Files.readString(foreign)).contains("not tapstate");
    }

    @Test
    void uninstallLeavesAForeignNameAlone(@TempDir Path binDir) throws IOException {
        // Symmetry with install: uninstall removes our shortcut, and refuses to delete a command it
        // never created. Removing it would be the same harm as overwriting it.
        Files.createFile(binDir.resolve("tapstate"));
        Path foreign = binDir.resolve("tap");
        Files.writeString(foreign, "#!/bin/sh\necho not tapstate\n");

        assertThatThrownBy(() -> AliasLinks.uninstall(binDir))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code().code())
                        .isEqualTo("TS0330006"));

        assertThat(Files.readString(foreign)).contains("not tapstate");
    }

    @Test
    void uninstallingWhatIsNotThereIsNotAnError(@TempDir Path binDir) throws IOException {
        // The end state the caller asked for already holds. Refusing here would make `uninstall` fail
        // on exactly the machines where the installer had already skipped the alias.
        Files.createFile(binDir.resolve("tapstate"));

        AliasLinks.uninstall(binDir);

        assertThat(Files.exists(binDir.resolve("tap"), java.nio.file.LinkOption.NOFOLLOW_LINKS)).isFalse();
    }
    @Test
    void aLinkPointingOutsideTheInstallDirectoryIsNotOurs(@TempDir Path binDir, @TempDir Path elsewhere)
            throws IOException {
        // Ownership was decided by the target's file name alone, so any link ending in "tapstate"
        // counted -- including one into a directory this installation does not own. install would then
        // replace it and uninstall would delete it, both silently, on a link somebody else put there
        // for their own reasons.
        Files.createFile(binDir.resolve("tapstate"));
        Files.createFile(elsewhere.resolve("tapstate"));
        Path foreign = binDir.resolve("tap");
        Files.createSymbolicLink(foreign, elsewhere.resolve("tapstate"));

        assertThatThrownBy(() -> AliasLinks.install(binDir))
                .isInstanceOfSatisfying(TapstateException.class, e ->
                        assertThat(e.code().code()).isEqualTo("TS0330006"));
        assertThatThrownBy(() -> AliasLinks.uninstall(binDir))
                .isInstanceOf(TapstateException.class);
        // Untouched, which is the point: refusing has to leave the other thing exactly as it was.
        assertThat(Files.readSymbolicLink(foreign)).isEqualTo(elsewhere.resolve("tapstate"));
    }

    @Test
    void theVersionedLinkTheInstallerWritesIsOurs(@TempDir Path binDir) throws IOException {
        // install.sh points the shortcut at versions/<v>/bin/tapstate rather than at the stable entry.
        // That is this installation's own link, one directory deeper, and treating it as foreign would
        // make `tapstate alias install` refuse on every machine the installer had already set up.
        Files.createFile(binDir.resolve("tapstate"));
        Path versioned = binDir.resolve("versions").resolve("0.3.0").resolve("bin");
        Files.createDirectories(versioned);
        Files.createFile(versioned.resolve("tapstate"));
        Files.createSymbolicLink(binDir.resolve("tap"), Path.of("versions/0.3.0/bin/tapstate"));

        AliasLinks.install(binDir);

        assertThat(Files.readSymbolicLink(binDir.resolve("tap"))).isEqualTo(Path.of("tapstate"));
    }

    @Test
    void aShortcutThatCannotBeWrittenIsRefusedWithACode(@TempDir Path binDir) {
        // An install directory that is not there is an ordinary thing a user's environment produces --
        // TAPSTATE_INSTALL_DIR naming a path nobody created. It is diagnosable and it is the user's to
        // act on, so it leaves through the error-code system like every other refusal this class makes.
        // An UncheckedIOException reaches the top as a stack trace with no code and no solution line,
        // on the same class that already codes the name-taken case one branch away.
        Path absent = binDir.resolve("no-such-dir");

        assertThatThrownBy(() -> AliasLinks.install(absent))
                .isInstanceOfSatisfying(TapstateException.class, e -> {
                    assertThat(e.code().code()).isEqualTo("TS0330008");
                    assertThat(e.args()).containsEntry("path", absent.resolve("tap").toString());
                    assertThat((String) e.args().get("reason")).isNotBlank();
                });
    }

    @Test
    void aShortcutThatCannotBeRemovedIsRefusedWithACode(@TempDir Path binDir) throws IOException {
        // The mirror side. Removing is the half a user reaches for when the shortcut is in the way, so
        // it must not be the half that answers with a stack trace.
        Files.createFile(binDir.resolve("tapstate"));
        AliasLinks.install(binDir);
        Path alias = binDir.resolve("tap");
        assertThat(binDir.toFile().setWritable(false)).as("the directory could be made read-only").isTrue();
        try {
            assertThatThrownBy(() -> AliasLinks.uninstall(binDir))
                    .isInstanceOfSatisfying(TapstateException.class, e -> {
                        assertThat(e.code().code()).isEqualTo("TS0330008");
                        assertThat(e.args()).containsEntry("path", alias.toString());
                    });
        } finally {
            binDir.toFile().setWritable(true);
        }
    }
}
