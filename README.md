# Toxicité des messages dans le tchat des streamers sur twitch


# Toxicité des messages dans le tchat des streamers sur twitch

## Contexte

Nous avons choisi de nous attaquer à une problématique majeure du web actuel : la toxicité dans les tchats Twitch. Avec des millions d'utilisateurs échangeant en direct, les flux de données provenant de l'extérieur sont massifs et souvent imprévisibles. En tant qu'étudiants en data science, notre objectif est de concevoir un système capable non seulement de détecter les insultes, mais aussi d'analyser les comportements malveillants sur le long terme pour aider les créateurs à protéger leur communauté.

C’est la première fois que nous mettons en œuvre une Architecture Lambda, et ce modèle nous a semblé idéal pour répondre aux contraintes de Twitch. Le défi technique réside dans la dualité du traitement : il faut être capable de réagir en quelques millisecondes pour modérer un message (vitesse), tout en étant capable de traiter des téraoctets de données historiques pour repérer des harceleurs récurrents (précision). L’architecture Lambda nous permet de ne pas sacrifier l’un pour l’autre en séparant le travail en deux circuits complémentaires.

Le cœur de notre dispositif commence par une phase d'ingestion robuste gérée par Apache Kafka. Kafka agit comme une plateforme de distribution qui reçoit les logs de tchats Twitch et les redirige simultanément vers nos deux couches de calcul. Pour la partie réactive, nous utilisons la "Real time layer" basée sur Spark Streaming. Cette couche traite les messages par micro-lots pour identifier immédiatement les pics de toxicité ou les mots interdits, permettant une mise à jour rapide des indicateurs de modération.

En parallèle, nous alimentons la "Batch layer" où les données brutes sont stockées de manière permanente sur Hadoop HDFS ou Amazon S3. Ici, nous utilisons Apache Spark  pour effectuer des calculs beaucoup plus lourds et précis sur l'historique complet des messages. Cette étape est cruciale car elle nous permet de corriger les approximations du temps réel et d'établir des profils de toxicité basés sur des semaines de logs, offrant ainsi une profondeur d'analyse impossible à obtenir en direct.

Enfin, la convergence de ces deux analyses se fait au niveau de la "Serving layer" , où nous avons choisi Cassandra  pour stocker et fusionner les résultats. Cette base de données distribue ensuite les informations vers notre interface de visualisation (Visu). Grâce à cette structure, l'utilisateur final dispose d'un tableau de bord complet qui combine la vigilance du direct et la fiabilité des données historiques, garantissant une modération à la fois rapide et intelligente.
