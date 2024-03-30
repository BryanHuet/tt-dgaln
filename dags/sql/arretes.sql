CREATE TABLE IF NOT EXISTS arretes(
    "unique_key_arrete_zone_alerte" TEXT PRIMARY KEY NOT NULL,
    "id_arrete" INTEGER NOT NULL,
    "id_zone" INTEGER NOT NULL,
    "numero_arrete" TEXT,
    "numero_arrete_cadre" TEXT,
    "date_signature" DATE,
    "debut_validite_arrete" DATE,
    "fin_validite_arrete" DATE,
    "numero_niveau" INTEGER,
    "nom_niveau" TEXT,
    "statut_arrete" TEXT,
    "chemin_fichier" TEXT,
    "chemin_fichier_arrete_cadre" TEXT
    
);