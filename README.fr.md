# Vyane

**Exécutez et coordonnez des tâches de modèles d’IA à partir d’une seule CLI et d’un noyau d’orchestration en Rust.**

[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md) · [한국어](README.ko.md) · [Español](README.es.md) · [Français](README.fr.md) · [Deutsch](README.de.md) · [Português (Brasil)](README.pt-BR.md)

Révision de référence : `b46c194` · Version du workspace : `0.1.0` · Mise à jour : 2026-10-15.
La version anglaise fait autorité. Cette traduction est un brouillon assisté par machine ; elle n’a pas été relue par une personne de langue maternelle française. Consultez la [politique de traduction](docs/TRANSLATING.md).

Vyane envoie une tâche à une cible configurée, la diffuse à plusieurs cibles ou essaie une chaîne explicite de cibles de secours. Les cibles peuvent utiliser les environnements d’exécution de Claude Code ou de Codex CLI, ou appeler directement des endpoints HTTP de modèles. Le projet s’adresse aux développeurs qui créent des workflows locaux avec des agents et des applications Rust nécessitant un routage explicite, des permissions d’exécution, l’annulation et des traces d’exécution.

![Des fenêtres de terminal abstraites sont reliées à des cartes de résultats par un centre de coordination cuivré.](docs/assets/vyane-hero.webp)

*Illustration conceptuelle générée par IA ; ce n’est pas un schéma d’architecture. [Notes sur la ressource](docs/assets/README.md).*

## État et limites

Ce projet est à un stade précoce, se compile à partir des sources et possède des API instables. La version Cargo est **0.1.0** ; les étiquettes de la feuille de route telles que v0.2–v0.4 désignent des jalons d’implémentation, et non des versions publiées. Au 2026-10-15, le dépôt ne comportait aucun tag Git ni aucune GitHub Release. Utilisez le démarrage rapide à partir des sources ci-dessous ; l’installation depuis un registre de paquets n’est pas un mode de distribution vérifié. La [liste de vérification avant publication](docs/RELEASING.md) indique un blocage actuel : 18 crates dans le workspace, mais seulement 17 entrées dans l’ordre de publication (`vyane-quota` manque).

| Disponible dans le code public | Limite |
| --- | --- |
| Envoi à une cible, diffusion à plusieurs cibles, bascule explicite en cas d’échec, streaming et historique local | Une cible HTTP directe fournit des réponses de modèle, pas d’outils d’accès au système de fichiers ou au shell. |
| Routage déterministe et workflows déclaratifs | Le routage dépend de vos profils ; il ne garantit pas automatiquement la qualité. |
| CLI, API REST sur l’interface de boucle locale et MCP sur stdio | Les interfaces exposent des sous-ensembles différents ; consultez le [guide de la CLI](docs/CLI-GUIDE.md). |
| Tâches persistantes, messages et objectifs locaux, et exécutions résidentes Process/Native sous Linux | Les exécutions Native démarrent à neuf et sans session ; la reprise après approbation, la reprise de session native et le rejeu automatique après redémarrage restent à réaliser. |
| Permissions explicites, environnements des processus enfants assainis et comptabilisation des exécutions | L’application du sandbox de la CLI est déléguée à l’environnement d’exécution sélectionné. Les champs de propriétaire et l’authentification sur la boucle locale n’assurent ni l’isolation face à des processus hostiles du même utilisateur ni l’isolation entre utilisateurs. |

Linux et macOS sont les plateformes de CI ; certaines fonctions d’exécution résidente et de sandbox sont spécifiques à Linux. La compatibilité avec Windows n’est pas revendiquée. L’accès aux modèles, les identifiants et toute CLI d’agent de programmation doivent être fournis séparément. Les appels aux fournisseurs peuvent entraîner des frais ; la compilation et les vérifications hors ligne ci-dessous n’appellent aucun modèle.

## Démarrage rapide

Installez Rust stable avec Cargo ainsi que les outils de compilation C/C++ de votre plateforme. La version minimale déclarée de Rust est 1.88. Le dépôt sélectionne le canal stable avec rustfmt et Clippy. Exécutez ces commandes depuis la racine du dépôt :

```sh
git clone https://github.com/zleo-ai/vyane-rs.git
cd vyane-rs
cargo install --locked --path crates/vyane-cli --root ./target/install
VYANE="$PWD/target/install/bin/vyane"
"$VYANE" --version
"$VYANE" --help
```

La version devrait afficher `vyane 0.1.0`. L’installation se fait dans cette copie du dépôt et ne remplace donc pas un autre `vyane` présent dans votre PATH. Gardez ce shell ouvert pour les commandes suivantes. Pour reproduire cet état de la documentation, sélectionnez la révision `b46c194` avant l’installation ; `main` peut évoluer.

### Première exécution sans identifiants

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

`check` devrait indiquer que la configuration est valide et comprend le profil `offline`. `route` renvoie du JSON avec `"selection_key": "offline"` et `"model": "offline-placeholder"` sans envoyer de tâche. Il s’agit d’un exemple de configuration hors ligne, pas d’une réponse de modèle simulée ni d’un fournisseur opérationnel ; n’envoyez pas de tâche à son endpoint volontairement invalide.

### Configurer une cible réelle

Créez `./target/model-config.toml` avec la structure suivante. Remplacez l’endpoint et le modèle d’exemple par des valeurs prises en charge par votre fournisseur ; `example.invalid` n’est volontairement pas un service opérationnel.

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

Définissez `VYANE_DEMO_API_KEY` via votre gestionnaire de secrets habituel ou l’environnement du shell ; ne placez jamais sa valeur dans le TOML et ne l’ajoutez jamais à un commit. Ensuite :

```sh
"$VYANE" --config ./target/model-config.toml check
"$VYANE" --config ./target/model-config.toml dispatch "Explain a Rust trait in one paragraph" --target chat
```

La première commande vérifie la configuration et la présence des identifiants ; elle ne prouve pas que l’endpoint accepte vos identifiants ou le modèle. La seconde envoie une véritable requête une fois les valeurs d’exemple remplacées. Le succès d’un appel à un fournisseur réel ne fait pas partie de la vérification du démarrage rapide hors ligne. Pour les environnements d’exécution d’agents de programmation, les permissions, les profils multiples et la bascule en cas d’échec, consultez les [exemples de configuration](profiles.example.toml) et le [guide de la CLI](docs/CLI-GUIDE.md).

## Fonctionnement des cibles

![La CLI, REST et MCP appellent le service et le noyau ; chaque cible associe fournisseur, protocole, environnement d’exécution et modèle ; les exécutions produisent des traces.](docs/assets/target-flow.svg)

| Champ | Signification |
| --- | --- |
| `provider` | Endpoint, source des identifiants et identité de facturation. |
| `protocol` | Format des échanges : OpenAI Chat, OpenAI Responses ou Anthropic Messages. |
| `harness` | Adaptateur d’exécution tel que Claude Code ou Codex CLI ; `none` signifie HTTP direct. |
| `model` | Identifiant de modèle dans l’espace de noms de ce fournisseur. |

Un profil associe ces quatre champs. Chaque cible de secours est résolue séparément : un identifiant de modèle n’est donc pas réutilisé silencieusement auprès d’un autre fournisseur. Le noyau dépend de traits partagés ; le service assemble les adaptateurs concrets. Consultez l’[architecture](docs/ARCHITECTURE.md) pour les détails sur les dépendances et les limites d’autorité.

## Exemples et référence

Après avoir configuré les profils indiqués, ces commandes illustrent les étapes suivantes :

```sh
"$VYANE" --config ./target/model-config.toml dispatch "Explain this algorithm" --target chat --stream
"$VYANE" --config ./target/model-config.toml broadcast "Compare two implementation approaches" --targets chat,review
"$VYANE" --config ./target/model-config.toml route "Review this design" --json
```

L’exemple de diffusion nécessite un second profil nommé `review`. Les prompts sont du texte littéral : mentionner un fichier n’ajoute pas son contenu à une requête HTTP directe. Le [guide de la CLI](docs/CLI-GUIDE.md) couvre les workflows, le cycle de vie du daemon, l’authentification REST, MCP, la messagerie locale et les objectifs. Utilisez `--help` sur chaque sous-commande pour consulter les commandes et options actuelles.

## Lien avec Python Vyane

Ce dépôt est l’implémentation publique en Rust de capacités du système privé d’origine Python Vyane. Il ne s’agit pas d’un remplacement à l’identique : les commandes Python, la configuration, les sessions enregistrées, les workflows et les intégrations opérationnelles doivent être vérifiés individuellement avant toute migration. Ne faites pas pointer les deux implémentations vers le même répertoire d’état sans procédure de migration documentée.

La [matrice publique des capacités](docs/parity/ORIGINAL-VYANE-PARITY.md) suit 53 capacités réparties en huit domaines : 7 implémentées, 23 partielles, 12 absentes, 9 volontairement différentes ou en attente d’une décision, et 2 prévues. Le [manifeste assaini](docs/parity/fixtures/v1/manifest.json) contient 44 cas répartis dans 5 suites : 30 correspondances après normalisation et 14 différences non résolues. Il s’agit de vérifications de comportement délimitées, pas d’un pourcentage de parité de l’ensemble du système. Recalculez les éléments de preuve de l’implémentation publique en Rust avec :

```sh
python3 .github/scripts/parity-report.py --format markdown
```

Cette commande exécute les vérifications des fixtures Rust et valide l’attestation assainie enregistrée ; elle n’exécute pas l’implémentation privée de référence. Les contributeurs publics n’ont pas besoin d’accéder au code privé. Consultez la [feuille de route](docs/ROADMAP.md) pour les travaux restant à réaliser et le [journal des modifications](docs/CHANGELOG.md) pour les changements du dépôt public.

## Contribuer et obtenir de l’aide

Commencez par [CONTRIBUTING.md](CONTRIBUTING.md) pour les vérifications et les règles de revue. Les contributions utiles comprennent les rapports de bugs reproductibles, les exemples plus petits, les corrections de traduction et les tests des capacités manquantes documentées. Recherchez dans les [Issues](https://github.com/zleo-ai/vyane-rs/issues) et les [PR ouvertes](https://github.com/zleo-ai/vyane-rs/pulls) avant de commencer un travail susceptible de recouper un effort existant. Indiquez la révision du code source, le système d’exploitation, la version de Rust, une reproduction minimale et une sortie assainie. N’incluez jamais de clés, de prompts privés ni de chemins personnels du système de fichiers.

Pour les vulnérabilités, suivez [SECURITY.md](SECURITY.md) avant de publier des détails. La couverture et l’état de relecture des traductions figurent dans [TRANSLATING.md](docs/TRANSLATING.md) ; la maintenance des versions et des publications figure dans [RELEASING.md](docs/RELEASING.md).

## Licence

Double licence [MIT](LICENSE-MIT) ou [Apache-2.0](LICENSE-APACHE), au choix. Les contributions sont soumises aux mêmes conditions ; consultez le [guide de contribution](CONTRIBUTING.md#licensing).
