# Vyane

**Ejecuta agentes de programación y API de modelos desde una sola CLI en Rust.**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

Para desarrolladores que utilizan varios agentes de programación o proveedores de modelos: asigna nombres a tus destinos, envía una tarea a uno o varios y conserva juntos los resultados y el historial de intentos. Usa Claude Code o Codex CLI para programar, o llama directamente a una API de modelos para obtener texto.

> **Desarrollo inicial.** Solo se instala desde el código fuente; las API son inestables y algunas capacidades siguen incompletas o ausentes. La reanudación de sesiones nativas y la repetición automática tras un reinicio no están terminadas. Consulta los [límites actuales](docs/PROJECT-STATUS.md).

## Mira cómo funciona

[Ejecuta la demostración de terminal](docs/EXAMPLES.md): la CLI instalada ejecuta una tarea, la envía a dos destinos en paralelo y se recupera de un fallo programado del destino principal. Utiliza respuestas HTTP de prueba locales, no necesita claves y no llama a modelos reales.

```text
Local HTTP fixtures only; no live model calls.
dispatch: fixture: chat-model; recorded attempts=1
broadcast: 2 separate successful results (chat-model, review-model)
failover: fixture: review-model; recorded attempts=2
```

## Tres formas de usarlo

| Si quieres… | Usa | Obtienes |
| --- | --- | --- |
| Ejecutar una tarea con un agente o modelo elegido | [`dispatch --target chat`](docs/EXAMPLES.md#one-task-one-target) | Un resultado y un registro local. Los adaptadores de agentes requieren instalación y configuración por separado. |
| Comparar respuestas al mismo prompt | [`broadcast --targets chat,review`](docs/EXAMPLES.md#one-prompt-several-targets) | Ejecuciones simultáneas con resultados separados; tú evalúas las respuestas. |
| Probar un respaldo configurado cuando falla una llamada | [`dispatch --target resilient`](docs/EXAMPLES.md#an-explicit-backup) | Una cadena ordenada de destinos alternativos para errores elegibles, con cada intento registrado. No transfiere la sesión. |

## Inicio rápido

**1. Instala desde el código fuente.** Necesitas Rust/Cargo estable (Rust 1.88 como mínimo) y herramientas de compilación C/C++. La CI cubre Linux y macOS; no se declara compatibilidad con Windows.

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
```

Versión esperada: `vyane 0.1.0`. El binario permanece dentro de esta copia del repositorio, sin reemplazar otro `vyane` en tu PATH. Mantén abierta esta shell.

**2. Obtén un primer resultado sin claves.** El ejemplo incluido solo previsualiza el enrutamiento; no envía solicitudes a modelos.

```sh
"$VYANE" --config docs/examples/offline.toml check
"$VYANE" --config docs/examples/offline.toml route "Explain this function" --json
```

Busca el perfil `offline` y `"selection_key": "offline"` en el JSON. El endpoint es inválido a propósito; no le envíes tareas.

**3. Conecta tu propio destino.** Sigue la [configuración del proveedor](docs/QUICKSTART.md#configure-a-real-target) y prueba los [tres ejemplos](docs/EXAMPLES.md). Debes aportar acceso al modelo y credenciales; las llamadas reales pueden generar cargos del proveedor. La vista previa sin conexión no valida un proveedor real.

## Qué funciona hoy

| Disponible en el código fuente | Límite |
| --- | --- |
| Ejecución individual, envío paralelo, conmutación explícita y streaming | HTTP directo devuelve texto del modelo; no adjunta archivos locales ni proporciona herramientas de shell. |
| Adaptadores de Claude Code y Codex CLI | Subprocesos CLI, sin integración de Codex App Server RPC ni integración directa de Claude Agent SDK. Tú aportas la CLI y el acceso; el adaptador aplica su sandbox. |
| Enrutamiento por perfiles e historial local | El enrutamiento sigue tu configuración; no garantiza la calidad de las respuestas. |
| Entradas CLI, REST y MCP | Cada una expone un subconjunto diferente. Consulta la [guía de CLI](docs/CLI-GUIDE.md). |

La ejecución residente, los flujos persistentes y los objetivos locales siguen evolucionando. Las ejecuciones nativas empiezan de cero y no tienen sesión; siguen pendientes la reanudación de aprobaciones y sesiones nativas y la repetición automática tras un reinicio. Los campos de propietario y la autenticación de loopback no aíslan accesos hostiles del mismo usuario ni de varios usuarios. Consulta el [estado y las limitaciones](docs/PROJECT-STATUS.md) antes de desarrollar sobre estas funciones.

## Más información

- [Arquitectura y modelo de destinos](docs/ARCHITECTURE.md) · [Referencia de configuración](profiles.example.toml) · [Guía de CLI, REST y MCP](docs/CLI-GUIDE.md)
- [Matriz de capacidades y evidencia reproducible](docs/PROJECT-STATUS.md#relationship-to-python-vyane) · [Hoja de ruta](docs/ROADMAP.md)
- [Registro de cambios](docs/CHANGELOG.md) · [Política de versiones y publicaciones](docs/RELEASING.md)

<details>
<summary>Resumen de la evidencia</summary>

La [matriz pública de capacidades](docs/parity/ORIGINAL-VYANE-PARITY.md) sigue 53 capacidades en 8 áreas: 7 implementadas, 23 parciales, 12 ausentes, 9 deliberadamente diferentes o pendientes de decisión y 2 planificadas. El [manifiesto sin datos sensibles](docs/parity/fixtures/v1/manifest.json) contiene 44 casos en 5 conjuntos: 30 coincidencias tras normalización y 14 diferencias abiertas. Estas comprobaciones acotadas no son un porcentaje de paridad de todo el sistema.

</details>

Esta es la implementación pública en Rust de capacidades del sistema privado original Python Vyane. No es un reemplazo directo: hay que comprobar por separado la migración de comandos, configuración, sesiones guardadas y flujos. No compartas directorios de estado sin un procedimiento de migración documentado. Los colaboradores públicos no necesitan el código privado.

## Contribuye y pide ayuda

Prueba un ejemplo, informa de un problema reproducible, mejora una traducción o aborda una carencia documentada. Revisa [Issues](https://github.com/zleo-ai/vyane-rs/issues) y los [PR abiertos](https://github.com/zleo-ai/vyane-rs/pulls), y lee [CONTRIBUTING.md](CONTRIBUTING.md). Incluye una revisión del código y una salida sin datos sensibles; omite claves, prompts privados y rutas personales. Notifica vulnerabilidades mediante [SECURITY.md](SECURITY.md).

## Licencia

[MIT](LICENSE-MIT) o [Apache-2.0](LICENSE-APACHE), a tu elección. Las contribuciones tienen las mismas condiciones.

---

Base del código: `fe43941` · Workspace: `0.1.0` · Actualizado: 2026-10-15. El inglés es la fuente de referencia; esta traducción asistida por máquina está pendiente de revisión humana por hablantes nativos. Consulta la [cobertura y el estado de las traducciones](docs/TRANSLATING.md). Los hitos de la hoja de ruta no son versiones publicadas.
