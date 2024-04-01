DROP VIEW IF EXISTS zones_arretes;

CREATE VIEW zones_arretes AS (
    SELECT z.nom_departement, z.surface_zone, z.type_zone, z.code_iso_departement,
    a.numero_niveau, a.date_signature, a.debut_validite_arrete, a.fin_validite_arrete, a.id_arrete
    FROM zones z
    JOIN arretes a ON z.id_zone = a.id_zone
    );
