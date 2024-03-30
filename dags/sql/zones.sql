CREATE TABLE IF NOT EXISTS zones(
    "id_zone" INTEGER PRIMARY KEY NOT NULL,
    "code_zone" TEXT,
    "type_zone" TEXT,
    "nom_zone" TEXT,
    "surface_zone" NUMERIC,
    "numero_version" INTEGER,
    "est_version_actuelle" BOOLEAN,
    "code_departement" TEXT,
    "code_iso_departement" TEXT,
    "nom_departement" TEXT,
    "surface_departement" NUMERIC,
    "nom_bassin_versant" TEXT
    
);