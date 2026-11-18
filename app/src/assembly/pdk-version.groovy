import groovy.json.JsonOutput
import java.nio.file.Files
import java.security.MessageDigest

def dependencies = project.runtimeArtifacts.findAll {
    it.groupId == 'io.tapdata' && it.type == 'jar'
}.sort { it.artifactId }.collect { artifact ->
    def digest = MessageDigest.getInstance('SHA-256')
    artifact.file.withInputStream { stream ->
        new java.security.DigestInputStream(stream, digest).transferTo(OutputStream.nullOutputStream())
    }
    def entry = [groupId: artifact.groupId, artifactId: artifact.artifactId,
                 version: artifact.baseVersion, resolvedVersion: artifact.version,
                 sha256: HexFormat.of().formatHex(digest.digest())]
    def snapshot = artifact.version =~ /-(\d{8}\.\d{6})-\d+$/
    if (snapshot.find()) {
        entry.snapshotTimestamp = snapshot.group(1)
    }
    entry
}
def api = dependencies.find { it.artifactId == 'tapdata-pdk-api' }
if (api == null) {
    throw new IllegalStateException('Missing host tapdata-pdk-api dependency')
}
def output = new File(project.build.outputDirectory, 'META-INF/tapstate/pdk-version.json').toPath()
Files.createDirectories(output.parent)
Files.writeString(output, JsonOutput.prettyPrint(JsonOutput.toJson(
        [pdkApiVersion: api.version, dependencies: dependencies])) + '\n')
