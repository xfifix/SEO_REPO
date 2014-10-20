## index pour la table de pricing du mot clé
CREATE INDEX ON pricing_keywords (keyword);
CREATE INDEX ON pricing_keywords (domain);
CREATE INDEX ON pricing_keywords (magasin);
CREATE INDEX ON pricing_keywords (url);


## index pour la table de pricing du mot clé
CREATE INDEX ON referential_keywords (keyword);

CREATE INDEX ON keywords (url);
CREATE INDEX ON keywords (domain);


CREATE INDEX ON amazon_pricing (keyword);
CREATE INDEX ON cdiscount_pricing (keyword);
CREATE INDEX ON searchdexing_keywords (keyword);