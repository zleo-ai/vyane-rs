# Vyane

**Coding-Agenten und Modell-APIs über eine einzige Rust-CLI ausführen.**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

Für Entwickler, die mehrere Coding-Agenten oder Modellanbieter nutzen: Gib deinen Zielen Namen, sende eine Aufgabe an eines oder mehrere davon und bewahre Ergebnisse und Versuchshistorie gemeinsam auf. Nutze Claude Code oder Codex CLI für Programmieraufgaben oder rufe eine Modell-API direkt für Textantworten auf.

> **Frühe Entwicklung.** Installation nur aus dem Quellcode; die APIs sind instabil und einige Funktionen sind unvollständig oder fehlen. Die Fortsetzung nativer Sitzungen und die automatische Wiederholung nach einem Neustart sind noch nicht fertig. Beachte die [aktuellen Grenzen](docs/PROJECT-STATUS.md).

## In Aktion ansehen

[Terminal-Demo ausführen](docs/EXAMPLES.md): Die installierte CLI führt eine Aufgabe aus, sendet sie parallel an zwei Ziele und wechselt nach einem gezielt ausgelösten Fehler des primären Ziels erfolgreich zum Ersatzziel. Die Demo nutzt lokale HTTP-Testantworten, benötigt keine Schlüssel und ruft keine echten Modelle auf.

```text
Local HTTP fixtures only; no live model calls.
dispatch: fixture: chat-model; recorded attempts=1
broadcast: 2 separate successful results (chat-model, review-model)
failover: fixture: review-model; recorded attempts=2
```

## Drei Einsatzmöglichkeiten

| Wenn du … möchtest | Befehl | Ergebnis |
| --- | --- | --- |
| Eine Aufgabe mit einem gewählten Agenten oder Modell ausführen | [`dispatch --target chat`](docs/EXAMPLES.md#one-task-one-target) | Ein Ergebnis und ein lokaler Laufdatensatz. Coding-Harnesses müssen separat installiert und eingerichtet werden. |
| Antworten auf denselben Prompt vergleichen | [`broadcast --targets chat,review`](docs/EXAMPLES.md#one-prompt-several-targets) | Gleichzeitige Ausführungen mit getrennten Ergebnissen; du bewertest die Antworten. |
| Nach einem fehlgeschlagenen Aufruf ein konfiguriertes Ersatzziel versuchen | [`dispatch --target resilient`](docs/EXAMPLES.md#an-explicit-backup) | Eine geordnete Failover-Kette für dafür vorgesehene Fehler; jeder Versuch wird erfasst. Das ist keine Sitzungsübergabe. |

## Schnellstart

**1. Aus dem Quellcode installieren.** Erforderlich sind stabiles Rust/Cargo (mindestens Rust 1.88) und eine C/C++-Build-Toolchain. Die CI läuft auf Linux und macOS; Windows-Unterstützung wird nicht zugesichert.

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
```

Erwartete Version: `vyane 0.1.0`. Die Binärdatei bleibt in dieser Repository-Kopie und ersetzt kein vorhandenes `vyane` im PATH. Lass diese Shell geöffnet.

**2. Ein erstes Ergebnis ohne Schlüssel erhalten.** Das mitgelieferte Beispiel zeigt nur eine Routing-Vorschau; es sendet keine Modellanfrage.

```sh
"$VYANE" --config docs/examples/offline.toml check
"$VYANE" --config docs/examples/offline.toml route "Explain this function" --json
```

Achte auf das Profil `offline` und `"selection_key": "offline"` im JSON. Der Endpunkt ist absichtlich ungültig; sende keine Aufgaben dorthin.

**3. Eigenes Ziel verbinden.** Folge der [Anbietereinrichtung](docs/QUICKSTART.md#configure-a-real-target) und probiere die [drei Beispiele](docs/EXAMPLES.md). Modellzugang und Zugangsdaten stellst du selbst bereit; echte Aufrufe können Kosten beim Anbieter verursachen. Die Offline-Vorschau prüft keinen echten Anbieter.

## Was heute funktioniert

| Im Quellcode verfügbar | Grenze |
| --- | --- |
| Einzelausführung, parallele Verteilung, explizites Failover und Streaming | Direktes HTTP liefert Modelltext; es hängt keine lokalen Dateien an und stellt keine Shell-Werkzeuge bereit. |
| Adapter für Claude Code und Codex CLI | CLI-Unterprozesse, keine Codex-App-Server-RPC- oder direkte Claude-Agent-SDK-Integration. CLI und Zugang stellst du selbst bereit; der Harness setzt seine Sandbox durch. |
| Profilbasiertes Routing und lokale Laufhistorie | Routing folgt deiner Konfiguration; es garantiert keine Antwortqualität. |
| CLI-, REST- und MCP-Einstiegspunkte | Jeder stellt eine andere Teilmenge bereit. Siehe [CLI-Handbuch](docs/CLI-GUIDE.md). |

Residente Ausführung, persistente Workflows und lokale Ziele werden weiterentwickelt. Native Läufe beginnen jeweils neu und haben keine Sitzung; die Fortsetzung nativer Freigaben und Sitzungen sowie die automatische Wiederholung nach einem Neustart sind noch offen. Eigentümerfelder und Loopback-Authentifizierung isolieren keine böswilligen Zugriffe desselben oder mehrerer Benutzer. Lies [Status und Einschränkungen](docs/PROJECT-STATUS.md), bevor du auf diesen Funktionen aufbaust.

## Weiterführende Informationen

- [Architektur und Zielmodell](docs/ARCHITECTURE.md) · [Konfigurationsreferenz](profiles.example.toml) · [CLI-, REST- und MCP-Handbuch](docs/CLI-GUIDE.md)
- [Funktionsmatrix und reproduzierbare Nachweise](docs/PROJECT-STATUS.md#relationship-to-python-vyane) · [Roadmap](docs/ROADMAP.md)
- [Änderungsprotokoll](docs/CHANGELOG.md) · [Versions- und Veröffentlichungsrichtlinien](docs/RELEASING.md)

Dies ist die öffentliche Rust-Implementierung von Funktionen des ursprünglichen privaten Python-Vyane-Systems. Sie ist kein direkt austauschbarer Ersatz: Befehle, Konfiguration, gespeicherte Sitzungen und Workflows erfordern jeweils eigene Migrationsprüfungen. Nutze Zustandsverzeichnisse nicht gemeinsam, solange kein dokumentierter Migrationsweg vorliegt. Öffentliche Mitwirkende benötigen keinen Zugriff auf den privaten Quellcode.

## Mitwirken und Hilfe erhalten

Probiere ein Beispiel, melde ein reproduzierbares Problem, verbessere eine Übersetzung oder schließe eine dokumentierte Funktionslücke. Prüfe [Issues](https://github.com/zleo-ai/vyane-rs/issues) und [offene PRs](https://github.com/zleo-ai/vyane-rs/pulls) und lies [CONTRIBUTING.md](CONTRIBUTING.md). Nenne eine Quellcode-Revision und bereinigte Ausgaben; lasse Schlüssel, private Prompts und persönliche Pfade weg. Melde Sicherheitslücken gemäß [SECURITY.md](SECURITY.md).

## Lizenz

Wahlweise [MIT](LICENSE-MIT) oder [Apache-2.0](LICENSE-APACHE). Für Beiträge gelten dieselben Bedingungen.

---

Quellcode-Basis: `fe43941` · Workspace: `0.1.0` · Aktualisiert: 2026-10-15. Englisch ist maßgeblich; diese maschinell unterstützte Übersetzung wartet auf eine menschliche Prüfung durch Muttersprachler. Siehe [Übersetzungsumfang und Prüfstatus](docs/TRANSLATING.md). Roadmap-Meilensteine sind keine veröffentlichten Releases.
