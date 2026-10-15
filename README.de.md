# Vyane

**KI-Modellaufgaben mit einer einzigen Rust-CLI und einem Orchestrierungskern ausführen und koordinieren.**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

Quellcode-Basis: `b46c194` · Workspace-Version: `0.1.0` · Aktualisiert: 2026-10-15.
Die englische Fassung ist maßgeblich. Diese Übersetzung ist ein maschinell unterstützter Entwurf; eine muttersprachliche Prüfung hat nicht stattgefunden. Siehe die [Übersetzungsrichtlinie](docs/TRANSLATING.md).

Vyane sendet eine Aufgabe an ein konfiguriertes Ziel, verteilt sie an mehrere Ziele oder versucht eine ausdrücklich festgelegte Kette von Ausweichzielen. Ziele können die Ausführungsumgebungen von Claude Code oder Codex CLI verwenden oder HTTP-Modellendpunkte direkt aufrufen. Das Projekt richtet sich an Entwickler, die lokale Agenten-Workflows und Rust-Anwendungen erstellen und dafür explizites Routing, Ausführungsberechtigungen, Abbruchmöglichkeiten und Ausführungsprotokolle benötigen.

![Abstrakte Terminalfenster sind über einen kupferfarbenen Koordinationsknoten mit Ergebniskarten verbunden.](docs/assets/vyane-hero.webp)

*KI-generierte Konzeptillustration; kein Architekturdiagramm. [Hinweise zur Grafik](docs/assets/README.md).*

## Status und Grenzen

Das Projekt befindet sich in einer frühen Phase, wird aus dem Quellcode gebaut und hat instabile APIs. Die Cargo-Version ist **0.1.0**; Roadmap-Bezeichnungen wie v0.2–v0.4 stehen für Implementierungsmeilensteine, nicht für veröffentlichte Versionen. Am 2026-10-15 gab es im Repository weder Git-Tags noch GitHub Releases. Verwende die folgende Anleitung zum Einstieg aus dem Quellcode; die Installation aus einer Paket-Registry ist kein verifizierter Distributionsweg. Die [Release-Checkliste](docs/RELEASING.md) dokumentiert einen aktuellen Blocker: 18 Crates im Workspace, aber nur 17 Einträge in der Veröffentlichungsreihenfolge (`vyane-quota` fehlt).

| Im öffentlichen Quellcode verfügbar | Grenze |
| --- | --- |
| Versand an ein Ziel, Verteilung an mehrere Ziele, explizites Failover, Streaming und lokaler Verlauf | Ein direktes HTTP-Ziel liefert Modellantworten, keine Dateisystem- oder Shell-Werkzeuge. |
| Deterministisches Routing und deklarative Workflows | Das Routing hängt von deinen Profilen ab; es garantiert nicht automatisch die Qualität. |
| CLI, REST-API über die Loopback-Schnittstelle und MCP über stdio | Die Frontends stellen unterschiedliche Teilmengen der Funktionen bereit; siehe die [CLI-Anleitung](docs/CLI-GUIDE.md). |
| Persistente Aufgaben, lokale Nachrichten und Ziele sowie residente Process/Native-Ausführungen unter Linux | Native-Ausführungen starten neu und ohne Sitzung; die Fortsetzung nach einer Freigabe, die Wiederaufnahme nativer Sitzungen und die automatische erneute Ausführung nach einem Neustart sind noch offen. |
| Explizite Berechtigungen, bereinigte Umgebungen für Kindprozesse und Erfassung von Ausführungsdaten | Die Durchsetzung der CLI-Sandbox wird an die gewählte Ausführungsumgebung delegiert. Eigentümerfelder und Loopback-Authentifizierung bieten weder Schutz vor feindlichen Prozessen desselben Benutzers noch eine Isolation zwischen Benutzern. |

Linux und macOS sind die CI-Plattformen; einige Funktionen für residente Ausführung und Sandboxing sind Linux-spezifisch. Windows-Unterstützung wird nicht zugesichert. Modellzugang, Zugangsdaten und gegebenenfalls eine Coding-Agent-CLI müssen separat bereitgestellt werden. Anbieteraufrufe können Kosten verursachen; der Build und die folgenden Offline-Prüfungen rufen keine Modelle auf.

## Schnellstart

Installiere Rust Stable mit Cargo sowie die C/C++-Build-Werkzeuge für deine Plattform. Die deklarierte Mindestversion von Rust ist 1.88. Das Repository wählt den Stable-Kanal mit rustfmt und Clippy. Führe die folgenden Befehle vom Stammverzeichnis des Repositorys aus:

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
"$VYANE" --help
```

Die Versionsausgabe sollte `vyane 0.1.0` lauten. Die Installation erfolgt innerhalb dieser Arbeitskopie und ersetzt daher kein anderes `vyane` in deinem PATH. Lass diese Shell für die folgenden Befehle geöffnet. Um diesen Dokumentationsstand zu reproduzieren, wechsle vor der Installation zur Revision `b46c194`; `main` kann sich ändern.

### Erster Start ohne Zugangsdaten

```sh
cat > ./target/offline-config.toml <<'TOML'
[providers.offline]
base_url = "https://example.invalid/v1"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "offline-placeholder"

[profiles.offline]
provider = "offline"
harness = "none"
model = "offline-placeholder"
TOML
"$VYANE" --config ./target/offline-config.toml check
"$VYANE" --config ./target/offline-config.toml route "Explain this function" --json
```

`check` sollte eine gültige Konfiguration mit dem Profil `offline` melden. `route` gibt JSON mit `"selection_key": "offline"` und `"model": "offline-placeholder"` zurück, ohne eine Aufgabe zu versenden. Dies ist ein Beispiel für eine Offline-Konfiguration, keine simulierte Modellantwort und kein funktionierender Anbieter; sende keine Aufgaben an den absichtlich ungültigen Endpunkt.

### Ein echtes Ziel konfigurieren

Erstelle `./target/model-config.toml` mit der folgenden Struktur. Ersetze den Beispielendpunkt und das Modell durch Werte, die dein Anbieter unterstützt; `example.invalid` ist absichtlich kein funktionsfähiger Dienst.

```toml
[providers.demo]
base_url = "https://example.invalid/v1"
api_key_env = "VYANE_DEMO_API_KEY"
auth_style = "bearer"
protocol = "openai_chat"
default_model = "YOUR_PROVIDER_MODEL_ID"

[profiles.chat]
provider = "demo"
protocol = "openai_chat"
harness = "none"
model = "YOUR_PROVIDER_MODEL_ID"
```

Setze `VYANE_DEMO_API_KEY` über deine übliche Geheimnisverwaltung oder die Shell-Umgebung; schreibe den Wert niemals in die TOML-Datei und nimm ihn niemals in einen Commit auf. Danach:

```sh
"$VYANE" --config ./target/model-config.toml check
"$VYANE" --config ./target/model-config.toml dispatch "Explain a Rust trait in one paragraph" --target chat
```

Der erste Befehl prüft die Konfiguration und das Vorhandensein von Zugangsdaten; er beweist nicht, dass der Endpunkt deine Zugangsdaten oder das Modell akzeptiert. Der zweite sendet eine echte Anfrage, sobald die Platzhalter ersetzt wurden. Ein erfolgreicher Aufruf bei einem echten Anbieter ist nicht Teil der Prüfung des Offline-Schnellstarts. Informationen zu Coding-Agent-Ausführungsumgebungen, Berechtigungen, mehreren Profilen und Failover findest du in den [Konfigurationsbeispielen](profiles.example.toml) und der [CLI-Anleitung](docs/CLI-GUIDE.md).

## Funktionsweise der Ziele

![CLI, REST und MCP rufen den Dienst und den Kern auf; jedes Ziel verknüpft Anbieter, Protokoll, Ausführungsumgebung und Modell; Ausführungen erzeugen Protokolle.](docs/assets/target-flow.svg)

| Feld | Bedeutung |
| --- | --- |
| `provider` | Endpunkt, Quelle der Zugangsdaten und Abrechnungsidentität. |
| `protocol` | Übertragungsformat: OpenAI Chat, OpenAI Responses oder Anthropic Messages. |
| `harness` | Ausführungsadapter wie Claude Code oder Codex CLI; `none` bedeutet direktes HTTP. |
| `model` | Eine Modellkennung im Namensraum dieses Anbieters. |

Ein Profil verknüpft diese vier Felder. Jedes Failover-Ziel wird separat aufgelöst, sodass eine Modellkennung nicht stillschweigend bei einem anderen Anbieter wiederverwendet wird. Der Kern hängt von gemeinsamen Traits ab; der Dienst stellt die konkreten Adapter zusammen. Details zu Abhängigkeiten und Zuständigkeitsgrenzen stehen in der [Architektur](docs/ARCHITECTURE.md).

## Beispiele und Referenz

Nach dem Konfigurieren der genannten Profile veranschaulichen diese Befehle die nächsten Schritte:

```sh
"$VYANE" --config ./target/model-config.toml dispatch "Explain this algorithm" --target chat --stream
"$VYANE" --config ./target/model-config.toml broadcast "Compare two implementation approaches" --targets chat,review
"$VYANE" --config ./target/model-config.toml route "Review this design" --json
```

Das Broadcast-Beispiel benötigt ein zweites Profil namens `review`. Prompts sind wörtlicher Text: Eine Datei zu erwähnen, hängt ihren Inhalt nicht an eine direkte HTTP-Anfrage an. Die [CLI-Anleitung](docs/CLI-GUIDE.md) behandelt Workflows, den Lebenszyklus des Daemons, REST-Authentifizierung, MCP, lokale Nachrichten und Ziele. Mit `--help` bei jedem Unterbefehl erhältst du die aktuell verfügbaren Befehle und Optionen.

## Verhältnis zu Python Vyane

Dieses Repository ist die öffentliche Rust-Implementierung von Fähigkeiten des ursprünglichen privaten Python-Vyane-Systems. Es ist kein unmittelbar austauschbarer Ersatz: Python-Befehle, Konfiguration, gespeicherte Sitzungen, Workflows und betriebliche Integrationen müssen vor einer Migration einzeln geprüft werden. Verwende für beide Implementierungen nicht dasselbe Zustandsverzeichnis, solange kein dokumentierter Migrationsweg besteht.

Die [öffentliche Fähigkeitsmatrix](docs/parity/ORIGINAL-VYANE-PARITY.md) erfasst 53 Fähigkeiten in acht Bereichen: 7 implementiert, 23 teilweise vorhanden, 12 fehlend, 9 bewusst abweichend oder noch nicht entschieden und 2 geplant. Das [bereinigte Manifest](docs/parity/fixtures/v1/manifest.json) enthält 44 Fälle in 5 Testsuiten: 30 normalisierte Übereinstimmungen und 14 offene Unterschiede. Dies sind begrenzte Verhaltensprüfungen, kein Paritätsprozentsatz für das Gesamtsystem. Berechne die öffentlichen Rust-Nachweise erneut mit:

```sh
python3 .github/scripts/parity-report.py --format markdown
```

Dies führt die Rust-Fixture-Prüfungen aus und validiert die gespeicherte, bereinigte Bestätigung; die private Referenzimplementierung wird dabei nicht ausgeführt. Öffentliche Mitwirkende benötigen keinen Zugriff auf privaten Quellcode. Offene Arbeiten findest du in der [Roadmap](docs/ROADMAP.md), Änderungen im öffentlichen Repository im [Changelog](docs/CHANGELOG.md).

## Mitwirken und Hilfe erhalten

Beginne mit [CONTRIBUTING.md](CONTRIBUTING.md), um die Prüfungen und Review-Regeln kennenzulernen. Hilfreiche Beiträge sind reproduzierbare Fehlerberichte, kleinere Beispiele, Übersetzungskorrekturen und Tests für dokumentierte Fähigkeitslücken. Suche in den [Issues](https://github.com/zleo-ai/vyane-rs/issues) und [offenen PRs](https://github.com/zleo-ai/vyane-rs/pulls), bevor du mit einer möglicherweise bereits bearbeiteten Aufgabe beginnst. Gib die Quellcode-Revision, das Betriebssystem, die Rust-Version, eine minimale Reproduktion und bereinigte Ausgaben an. Füge niemals Schlüssel, private Prompts oder persönliche Dateisystempfade hinzu.

Befolge bei Sicherheitslücken [SECURITY.md](SECURITY.md), bevor du Details veröffentlichst. Übersetzungsumfang und Prüfstatus stehen in [TRANSLATING.md](docs/TRANSLATING.md); Release- und Versionspflege sind in [RELEASING.md](docs/RELEASING.md) beschrieben.

## Lizenz

Wahlweise unter [MIT](LICENSE-MIT) oder [Apache-2.0](LICENSE-APACHE) doppelt lizenziert. Beiträge unterliegen denselben Bedingungen; siehe [Mitwirken](CONTRIBUTING.md#licensing).
