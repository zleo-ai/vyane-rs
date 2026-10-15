# Vyane

**Ejecuta y coordina tareas de modelos de IA desde una única CLI y un núcleo de orquestación en Rust.**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

Revisión de referencia: `b46c194` · Versión del workspace: `0.1.0` · Actualizado: 2026-10-15.
La versión en inglés es la referencia autorizada. Esta traducción es un borrador asistido por máquina; no ha sido revisada por hablantes nativos. Consulta la [política de traducción](docs/TRANSLATING.md).

Vyane envía una tarea a un destino configurado, la distribuye a varios o recorre una cadena explícita de destinos alternativos en caso de fallo. Los destinos pueden usar los entornos de ejecución de Claude Code o Codex CLI, o llamar directamente a endpoints HTTP de modelos. Está dirigido a desarrolladores que crean flujos de trabajo locales con agentes y aplicaciones Rust que necesitan enrutamiento explícito, permisos de ejecución, cancelación y registros de ejecución.

![Ventanas de terminal abstractas se conectan a tarjetas de resultados a través de un centro de coordinación de color cobre.](docs/assets/vyane-hero.webp)

*Ilustración conceptual generada por IA; no es un diagrama de arquitectura. [Notas sobre el recurso](docs/assets/README.md).*

## Estado y limitaciones

Este proyecto está en una fase temprana, se compila desde el código fuente y tiene API inestables. La versión de Cargo es **0.1.0**; las etiquetas de la hoja de ruta, como v0.2–v0.4, describen hitos de implementación, no versiones publicadas. A fecha de 2026-10-15, el repositorio no tenía etiquetas Git ni GitHub Releases. Usa la guía de inicio desde el código fuente que aparece abajo; la instalación desde un registro de paquetes no es una vía de distribución verificada. La [lista de comprobación de publicación](docs/RELEASING.md) recoge un bloqueo actual: hay 18 crates en el workspace, pero solo 17 entradas en el orden de publicación (falta `vyane-quota`).

| Disponible en el código público | Limitación |
| --- | --- |
| Envío a un destino, distribución a varios, conmutación explícita ante fallos, streaming e historial local | Un destino HTTP directo proporciona respuestas del modelo, no herramientas de sistema de archivos ni de shell. |
| Enrutamiento determinista y flujos de trabajo declarativos | El enrutamiento depende de tus perfiles; no garantiza automáticamente la calidad. |
| CLI, API REST sobre la interfaz de bucle local y MCP por stdio | Las interfaces exponen distintos subconjuntos de funciones; consulta la [guía de la CLI](docs/CLI-GUIDE.md). |
| Tareas persistentes, mensajes y objetivos locales, y ejecuciones residentes Process/Native en Linux | Las ejecuciones Native son nuevas y sin sesión; siguen pendientes la reanudación tras una aprobación, la reanudación de sesiones nativas y la repetición automática tras un reinicio. |
| Permisos explícitos, entornos de procesos hijos depurados y contabilización de ejecuciones | La aplicación del sandbox de la CLI se delega al entorno de ejecución seleccionado. Los campos de propietario y la autenticación en la interfaz de bucle local no proporcionan aislamiento frente a procesos hostiles del mismo usuario ni aislamiento entre usuarios. |

Linux y macOS son las plataformas de CI; algunas funciones de ejecución residente y sandbox son específicas de Linux. No se afirma que Windows sea compatible. El acceso a los modelos, las credenciales y cualquier CLI de agentes de programación deben proporcionarse por separado. Las llamadas a proveedores pueden generar costes; la compilación y las comprobaciones sin conexión descritas abajo no llaman a modelos.

## Inicio rápido

Instala Rust estable con Cargo y las herramientas de compilación C/C++ de tu plataforma. La versión mínima declarada de Rust es 1.88. El repositorio selecciona el canal estable con rustfmt y Clippy. Ejecuta lo siguiente desde la raíz del repositorio:

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
"$VYANE" --help
```

La versión debería mostrar `vyane 0.1.0`. La instalación se realiza dentro de esta copia del repositorio, por lo que no reemplaza otro `vyane` en tu PATH. Mantén esta shell abierta para los siguientes comandos. Para reproducir esta versión de la documentación, selecciona la revisión `b46c194` antes de instalar; `main` puede cambiar.

### Primera ejecución sin credenciales

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

`check` debería indicar que la configuración es válida e incluye el perfil `offline`. `route` devuelve JSON con `"selection_key": "offline"` y `"model": "offline-placeholder"` sin enviar la tarea. Este es un ejemplo de configuración sin conexión, no una respuesta simulada de un modelo ni un proveedor operativo; no envíes tareas a su endpoint deliberadamente inválido.

### Configurar un destino real

Crea `./target/model-config.toml` con la siguiente estructura. Sustituye el endpoint y el modelo de ejemplo por valores admitidos por tu proveedor; `example.invalid` no es un servicio operativo, de forma deliberada.

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

Configura `VYANE_DEMO_API_KEY` mediante tu gestor de secretos habitual o el entorno de la shell; nunca incluyas su valor en el TOML ni en un commit. Después:

```sh
"$VYANE" --config ./target/model-config.toml check
"$VYANE" --config ./target/model-config.toml dispatch "Explain a Rust trait in one paragraph" --target chat
```

El primer comando comprueba la configuración y la presencia de credenciales; no demuestra que el endpoint acepte tus credenciales o el modelo. El segundo envía una solicitud real una vez sustituidos los valores de ejemplo. El éxito de una llamada a un proveedor real no forma parte de la verificación del inicio rápido sin conexión. Para los entornos de ejecución de agentes de programación, los permisos, los perfiles múltiples y la conmutación ante fallos, consulta los [ejemplos de configuración](profiles.example.toml) y la [guía de la CLI](docs/CLI-GUIDE.md).

## Cómo funcionan los destinos

![La CLI, REST y MCP llaman al servicio y al núcleo; cada destino vincula proveedor, protocolo, entorno de ejecución y modelo; las ejecuciones generan registros.](docs/assets/target-flow.svg)

| Campo | Significado |
| --- | --- |
| `provider` | Endpoint, fuente de credenciales e identidad de facturación. |
| `protocol` | Formato de intercambio: OpenAI Chat, OpenAI Responses o Anthropic Messages. |
| `harness` | Adaptador de ejecución, como Claude Code o Codex CLI; `none` significa HTTP directo. |
| `model` | Identificador de modelo dentro del espacio de nombres de ese proveedor. |

Un perfil vincula estos cuatro campos. Cada destino de la cadena de conmutación ante fallos se resuelve por separado, por lo que un identificador de modelo no se reutiliza silenciosamente con otro proveedor. El núcleo depende de traits compartidos; el servicio ensambla adaptadores concretos. Consulta la [arquitectura](docs/ARCHITECTURE.md) para conocer las dependencias y los límites de autoridad.

## Ejemplos y referencia

Una vez configurados los perfiles indicados, estos comandos ilustran los siguientes pasos:

```sh
"$VYANE" --config ./target/model-config.toml dispatch "Explain this algorithm" --target chat --stream
"$VYANE" --config ./target/model-config.toml broadcast "Compare two implementation approaches" --targets chat,review
"$VYANE" --config ./target/model-config.toml route "Review this design" --json
```

El ejemplo de distribución requiere un segundo perfil llamado `review`. Los prompts son texto literal: mencionar un archivo no adjunta su contenido a una solicitud HTTP directa. La [guía de la CLI](docs/CLI-GUIDE.md) cubre los flujos de trabajo, el ciclo de vida del daemon, la autenticación REST, MCP, la mensajería local y los objetivos. Usa `--help` en cada subcomando para consultar las opciones actuales.

## Relación con Python Vyane

Este repositorio es la implementación pública en Rust de capacidades del sistema original privado Python Vyane. No es un reemplazo directo: los comandos de Python, la configuración, las sesiones almacenadas, los flujos de trabajo y las integraciones operativas deben comprobarse por separado antes de migrar. No apuntes ambas implementaciones al mismo directorio de estado sin una ruta de migración documentada.

La [matriz pública de capacidades](docs/parity/ORIGINAL-VYANE-PARITY.md) registra 53 capacidades en ocho ámbitos: 7 implementadas, 23 parciales, 12 ausentes, 9 deliberadamente diferentes o pendientes de una decisión y 2 planificadas. El [manifiesto depurado](docs/parity/fixtures/v1/manifest.json) contiene 44 casos en 5 suites: 30 coincidencias normalizadas y 14 diferencias pendientes. Son comprobaciones acotadas del comportamiento, no un porcentaje de paridad de todo el sistema. Vuelve a calcular las evidencias de la implementación pública en Rust con:

```sh
python3 .github/scripts/parity-report.py --format markdown
```

Esto ejecuta las comprobaciones de fixtures en Rust y valida la atestación depurada almacenada; no ejecuta la implementación privada de referencia. Los colaboradores públicos no necesitan acceso al código privado. Consulta la [hoja de ruta](docs/ROADMAP.md) para conocer el trabajo pendiente y el [registro de cambios](docs/CHANGELOG.md) para ver los cambios del repositorio público.

## Contribuir y obtener ayuda

Empieza por [CONTRIBUTING.md](CONTRIBUTING.md) para conocer las comprobaciones y las reglas de revisión. Entre las contribuciones útiles están los informes de errores reproducibles, los ejemplos más pequeños, las correcciones de traducción y las pruebas para las carencias de capacidades documentadas. Busca en [Issues](https://github.com/zleo-ai/vyane-rs/issues) y en las [PR abiertas](https://github.com/zleo-ai/vyane-rs/pulls) antes de empezar un trabajo que pueda solaparse con otro. Incluye la revisión del código fuente, el sistema operativo, la versión de Rust, una reproducción mínima y la salida depurada. Nunca incluyas claves, prompts privados ni rutas personales del sistema de archivos.

Para las vulnerabilidades, sigue [SECURITY.md](SECURITY.md) antes de publicar detalles. La cobertura y el estado de revisión de las traducciones están en [TRANSLATING.md](docs/TRANSLATING.md); el mantenimiento de versiones y publicaciones está en [RELEASING.md](docs/RELEASING.md).

## Licencia

Disponible bajo licencia dual [MIT](LICENSE-MIT) o [Apache-2.0](LICENSE-APACHE), a tu elección. Las contribuciones se rigen por los mismos términos; consulta la [guía de contribución](CONTRIBUTING.md#licensing).
