DECLARE
	@min_items_for_cluster as INT,
	@days_from_current_date as INT,
	@min_discount_ranges as INT,
	@min_factor as FLOAT,
	@max_factor as FLOAT;
SET @min_items_for_cluster = 20;
SET @days_from_current_date = 360;
SET @min_discount_ranges = 2;
SET @min_factor = 1.5;
SET @max_factor = 4;

IF OBJECT_ID('fator_base') IS NOT NULL
  DROP TABLE fator_base;
SELECT
COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros') as categoria,
LTRIM(RTRIM(p.fabricante)) as fabricante,
v.faixa_de_desconto,
1-SUM(receita)/SUM(preco_original_total) as desconto_medio,
SUM(v.itens)/CAST(dbo.GREATEST(15,DATEDIFF(DAY,MIN(v.primeira_venda),MAX(v.ultima_venda))) AS FLOAT) as giro
INTO dbo.fator_base
FROM (
	SELECT
	MIN(vp.data_venda) as primeira_venda,
	MAX(vp.data_venda) as ultima_venda,
	vp.produto,
	vp.cor_produto,
	CASE
		WHEN 1 - COALESCE(v.valor_pago/NULLIF(v.valor_venda_bruta,0),1)*vp.preco_liquido / (vp.preco_liquido + vp.desconto_item) < 0.2 THEN 2
		WHEN 1 - COALESCE(v.valor_pago/NULLIF(v.valor_venda_bruta,0),1)*vp.preco_liquido / (vp.preco_liquido + vp.desconto_item) < 0.4 THEN 4
		WHEN 1 - COALESCE(v.valor_pago/NULLIF(v.valor_venda_bruta,0),1)*vp.preco_liquido / (vp.preco_liquido + vp.desconto_item) < 0.6 THEN 6
		ELSE 8
	END as faixa_de_desconto,
	SUM(vp.qtde) as itens,
	SUM(vp.qtde*(vp.preco_liquido + vp.desconto_item)) as preco_original_total,
	SUM(vp.qtde*(COALESCE(v.valor_pago/NULLIF(v.valor_venda_bruta,0),1)*vp.preco_liquido)) as receita,
	SUM(vp.custo*vp.qtde) as custo
	FROM loja_venda_produto vp
	INNER JOIN loja_venda v on
		v.codigo_filial = vp.codigo_filial and
		v.ticket = vp.ticket and
		v.data_venda = vp.data_venda
	INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
	WHERE
	vp.qtde > 0 and
	vp.data_venda >= GETDATE()-@days_from_current_date
	GROUP BY
	vp.produto,
	vp.cor_produto,
	CASE
		WHEN 1 - COALESCE(v.valor_pago/NULLIF(v.valor_venda_bruta,0),1)*vp.preco_liquido / (vp.preco_liquido + vp.desconto_item) < 0.2 THEN 2
		WHEN 1 - COALESCE(v.valor_pago/NULLIF(v.valor_venda_bruta,0),1)*vp.preco_liquido / (vp.preco_liquido + vp.desconto_item) < 0.4 THEN 4
		WHEN 1 - COALESCE(v.valor_pago/NULLIF(v.valor_venda_bruta,0),1)*vp.preco_liquido / (vp.preco_liquido + vp.desconto_item) < 0.6 THEN 6
		ELSE 8
	END

	UNION

	-- Venda E-COMMMERCE:
	SELECT
	MIN(f.emissao) as primeira_venda,
	MAX(f.emissao) as ultima_venda,
	pb.produto as produto,
	pb.cor_produto as cor_produto,
	CASE
		WHEN 1 - (f.valor_total/f.valor_sub_itens*(fp.valor/fp.qtde)) / (fp.preco + fp.desconto_item) < 0.2 THEN 2
		WHEN 1 - (f.valor_total/f.valor_sub_itens*(fp.valor/fp.qtde)) / (fp.preco + fp.desconto_item) < 0.4 THEN 4
		WHEN 1 - (f.valor_total/f.valor_sub_itens*(fp.valor/fp.qtde)) / (fp.preco + fp.desconto_item) < 0.6 THEN 6
		ELSE 8
	END as faixa_de_desconto,
	SUM(CASE pb.tamanho
		WHEN 1 THEN fp.f1
		WHEN 2 THEN fp.f2
		WHEN 3 THEN fp.f3
		WHEN 4 THEN fp.f4
		WHEN 5 THEN fp.f5
		WHEN 6 THEN fp.f6
		WHEN 7 THEN fp.f7
		WHEN 8 THEN fp.f8
		WHEN 9 THEN fp.f9
		WHEN 10 THEN fp.f10
		WHEN 11 THEN fp.f11
		WHEN 12 THEN fp.f12
		WHEN 13 THEN fp.f13
		WHEN 14 THEN fp.f14
		WHEN 15 THEN fp.f15
		WHEN 16 THEN fp.f16
		WHEN 17 THEN fp.f17
		WHEN 18 THEN fp.f18
		WHEN 19 THEN fp.f19
		WHEN 20 THEN fp.f20
		WHEN 21 THEN fp.f21
		WHEN 22 THEN fp.f22
		WHEN 23 THEN fp.f23
		WHEN 24 THEN fp.f24
		WHEN 25 THEN fp.f25
		WHEN 26 THEN fp.f26
		WHEN 27 THEN fp.f27
		WHEN 28 THEN fp.f28
		WHEN 29 THEN fp.f29
		WHEN 30 THEN fp.f30
		WHEN 31 THEN fp.f31
		WHEN 32 THEN fp.f32
		WHEN 33 THEN fp.f33
		WHEN 34 THEN fp.f34
		WHEN 35 THEN fp.f35
		WHEN 36 THEN fp.f36
		WHEN 37 THEN fp.f37
		WHEN 38 THEN fp.f38
		WHEN 39 THEN fp.f39
		WHEN 40 THEN fp.f40
		WHEN 41 THEN fp.f41
		WHEN 42 THEN fp.f42
		WHEN 43 THEN fp.f43
		WHEN 44 THEN fp.f44
		WHEN 45 THEN fp.f45
		WHEN 46 THEN fp.f46
		WHEN 47 THEN fp.f47
		WHEN 48 THEN fp.f48
		ELSE 0
	END) as itens,
	SUM((fp.preco + fp.desconto_item)*
		CASE pb.tamanho
			WHEN 1 THEN fp.f1
			WHEN 2 THEN fp.f2
			WHEN 3 THEN fp.f3
			WHEN 4 THEN fp.f4
			WHEN 5 THEN fp.f5
			WHEN 6 THEN fp.f6
			WHEN 7 THEN fp.f7
			WHEN 8 THEN fp.f8
			WHEN 9 THEN fp.f9
			WHEN 10 THEN fp.f10
			WHEN 11 THEN fp.f11
			WHEN 12 THEN fp.f12
			WHEN 13 THEN fp.f13
			WHEN 14 THEN fp.f14
			WHEN 15 THEN fp.f15
			WHEN 16 THEN fp.f16
			WHEN 17 THEN fp.f17
			WHEN 18 THEN fp.f18
			WHEN 19 THEN fp.f19
			WHEN 20 THEN fp.f20
			WHEN 21 THEN fp.f21
			WHEN 22 THEN fp.f22
			WHEN 23 THEN fp.f23
			WHEN 24 THEN fp.f24
			WHEN 25 THEN fp.f25
			WHEN 26 THEN fp.f26
			WHEN 27 THEN fp.f27
			WHEN 28 THEN fp.f28
			WHEN 29 THEN fp.f29
			WHEN 30 THEN fp.f30
			WHEN 31 THEN fp.f31
			WHEN 32 THEN fp.f32
			WHEN 33 THEN fp.f33
			WHEN 34 THEN fp.f34
			WHEN 35 THEN fp.f35
			WHEN 36 THEN fp.f36
			WHEN 37 THEN fp.f37
			WHEN 38 THEN fp.f38
			WHEN 39 THEN fp.f39
			WHEN 40 THEN fp.f40
			WHEN 41 THEN fp.f41
			WHEN 42 THEN fp.f42
			WHEN 43 THEN fp.f43
			WHEN 44 THEN fp.f44
			WHEN 45 THEN fp.f45
			WHEN 46 THEN fp.f46
			WHEN 47 THEN fp.f47
			WHEN 48 THEN fp.f48
			ELSE 0
		END
	) as preco_original_total,
	SUM(f.valor_total/f.valor_sub_itens*(fp.valor/fp.qtde*
		CASE pb.tamanho
			WHEN 1 THEN fp.f1
			WHEN 2 THEN fp.f2
			WHEN 3 THEN fp.f3
			WHEN 4 THEN fp.f4
			WHEN 5 THEN fp.f5
			WHEN 6 THEN fp.f6
			WHEN 7 THEN fp.f7
			WHEN 8 THEN fp.f8
			WHEN 9 THEN fp.f9
			WHEN 10 THEN fp.f10
			WHEN 11 THEN fp.f11
			WHEN 12 THEN fp.f12
			WHEN 13 THEN fp.f13
			WHEN 14 THEN fp.f14
			WHEN 15 THEN fp.f15
			WHEN 16 THEN fp.f16
			WHEN 17 THEN fp.f17
			WHEN 18 THEN fp.f18
			WHEN 19 THEN fp.f19
			WHEN 20 THEN fp.f20
			WHEN 21 THEN fp.f21
			WHEN 22 THEN fp.f22
			WHEN 23 THEN fp.f23
			WHEN 24 THEN fp.f24
			WHEN 25 THEN fp.f25
			WHEN 26 THEN fp.f26
			WHEN 27 THEN fp.f27
			WHEN 28 THEN fp.f28
			WHEN 29 THEN fp.f29
			WHEN 30 THEN fp.f30
			WHEN 31 THEN fp.f31
			WHEN 32 THEN fp.f32
			WHEN 33 THEN fp.f33
			WHEN 34 THEN fp.f34
			WHEN 35 THEN fp.f35
			WHEN 36 THEN fp.f36
			WHEN 37 THEN fp.f37
			WHEN 38 THEN fp.f38
			WHEN 39 THEN fp.f39
			WHEN 40 THEN fp.f40
			WHEN 41 THEN fp.f41
			WHEN 42 THEN fp.f42
			WHEN 43 THEN fp.f43
			WHEN 44 THEN fp.f44
			WHEN 45 THEN fp.f45
			WHEN 46 THEN fp.f46
			WHEN 47 THEN fp.f47
			WHEN 48 THEN fp.f48
			ELSE 0
		END
	)) as receita_total,
	SUM(fp.custo_na_data*CASE pb.tamanho
		WHEN 1 THEN fp.f1
		WHEN 2 THEN fp.f2
		WHEN 3 THEN fp.f3
		WHEN 4 THEN fp.f4
		WHEN 5 THEN fp.f5
		WHEN 6 THEN fp.f6
		WHEN 7 THEN fp.f7
		WHEN 8 THEN fp.f8
		WHEN 9 THEN fp.f9
		WHEN 10 THEN fp.f10
		WHEN 11 THEN fp.f11
		WHEN 12 THEN fp.f12
		WHEN 13 THEN fp.f13
		WHEN 14 THEN fp.f14
		WHEN 15 THEN fp.f15
		WHEN 16 THEN fp.f16
		WHEN 17 THEN fp.f17
		WHEN 18 THEN fp.f18
		WHEN 19 THEN fp.f19
		WHEN 20 THEN fp.f20
		WHEN 21 THEN fp.f21
		WHEN 22 THEN fp.f22
		WHEN 23 THEN fp.f23
		WHEN 24 THEN fp.f24
		WHEN 25 THEN fp.f25
		WHEN 26 THEN fp.f26
		WHEN 27 THEN fp.f27
		WHEN 28 THEN fp.f28
		WHEN 29 THEN fp.f29
		WHEN 30 THEN fp.f30
		WHEN 31 THEN fp.f31
		WHEN 32 THEN fp.f32
		WHEN 33 THEN fp.f33
		WHEN 34 THEN fp.f34
		WHEN 35 THEN fp.f35
		WHEN 36 THEN fp.f36
		WHEN 37 THEN fp.f37
		WHEN 38 THEN fp.f38
		WHEN 39 THEN fp.f39
		WHEN 40 THEN fp.f40
		WHEN 41 THEN fp.f41
		WHEN 42 THEN fp.f42
		WHEN 43 THEN fp.f43
		WHEN 44 THEN fp.f44
		WHEN 45 THEN fp.f45
		WHEN 46 THEN fp.f46
		WHEN 47 THEN fp.f47
		WHEN 48 THEN fp.f48
		ELSE 0
	END) as custo_total
	FROM faturamento_prod fp
	INNER JOIN faturamento f on
		f.nf_saida = fp.NF_SAIDA and
		fp.filial = f.filial
	INNER JOIN produtos_barra pb on
		fp.produto = pb.produto and
		fp.cor_produto = pb.cor_produto
	WHERE
	CASE pb.tamanho
		WHEN 1 THEN fp.f1
		WHEN 2 THEN fp.f2
		WHEN 3 THEN fp.f3
		WHEN 4 THEN fp.f4
		WHEN 5 THEN fp.f5
		WHEN 6 THEN fp.f6
		WHEN 7 THEN fp.f7
		WHEN 8 THEN fp.f8
		WHEN 9 THEN fp.f9
		WHEN 10 THEN fp.f10
		WHEN 11 THEN fp.f11
		WHEN 12 THEN fp.f12
		WHEN 13 THEN fp.f13
		WHEN 14 THEN fp.f14
		WHEN 15 THEN fp.f15
		WHEN 16 THEN fp.f16
		WHEN 17 THEN fp.f17
		WHEN 18 THEN fp.f18
		WHEN 19 THEN fp.f19
		WHEN 20 THEN fp.f20
		WHEN 21 THEN fp.f21
		WHEN 22 THEN fp.f22
		WHEN 23 THEN fp.f23
		WHEN 24 THEN fp.f24
		WHEN 25 THEN fp.f25
		WHEN 26 THEN fp.f26
		WHEN 27 THEN fp.f27
		WHEN 28 THEN fp.f28
		WHEN 29 THEN fp.f29
		WHEN 30 THEN fp.f30
		WHEN 31 THEN fp.f31
		WHEN 32 THEN fp.f32
		WHEN 33 THEN fp.f33
		WHEN 34 THEN fp.f34
		WHEN 35 THEN fp.f35
		WHEN 36 THEN fp.f36
		WHEN 37 THEN fp.f37
		WHEN 38 THEN fp.f38
		WHEN 39 THEN fp.f39
		WHEN 40 THEN fp.f40
		WHEN 41 THEN fp.f41
		WHEN 42 THEN fp.f42
		WHEN 43 THEN fp.f43
		WHEN 44 THEN fp.f44
		WHEN 45 THEN fp.f45
		WHEN 46 THEN fp.f46
		WHEN 47 THEN fp.f47
		WHEN 48 THEN fp.f48
		ELSE 0
	END > 0 and
	f.valor_sub_itens > 0 and
	fp.serie_nf IN (2,7) and
	f.natureza_saida = '100.01' and
	f.filial = 'e-commerce' and
	f.emissao >= GETDATE()-@days_from_current_date
	GROUP BY
	pb.produto,
	pb.cor_produto,
	CASE
		WHEN 1 - (f.valor_total/f.valor_sub_itens*(fp.valor/fp.qtde)) / (fp.preco + fp.desconto_item) < 0.2 THEN 2
		WHEN 1 - (f.valor_total/f.valor_sub_itens*(fp.valor/fp.qtde)) / (fp.preco + fp.desconto_item) < 0.4 THEN 4
		WHEN 1 - (f.valor_total/f.valor_sub_itens*(fp.valor/fp.qtde)) / (fp.preco + fp.desconto_item) < 0.6 THEN 6
		ELSE 8
	END
) v
INNER JOIN produto_cores pc on
	pc.produto = v.produto and
	pc.cor_produto = v.cor_produto
INNER JOIN produtos p on p.produto = v.produto
LEFT JOIN bi_categorizacao_categoria ccc on
	ccc.grupo_produto = p.grupo_produto and
	ccc.cod_categoria = p.cod_categoria and
	ccc.subgrupo_produto is null and
	ccc.tipo_produto is null
LEFT JOIN bi_categorizacao_categoria cct on
	cct.grupo_produto = p.grupo_produto and
	cct.tipo_produto = p.tipo_produto and
	cct.subgrupo_produto is null and
	cct.cod_categoria is null
LEFT JOIN bi_categorizacao_categoria ccs on
	ccs.grupo_produto = p.grupo_produto and
	ccs.subgrupo_produto = p.subgrupo_produto and
	ccs.tipo_produto is null and
	ccs.cod_categoria is null
LEFT JOIN bi_categorizacao_categoria cc on
	cc.grupo_produto = p.grupo_produto and
	cc.subgrupo_produto is null and
	cc.tipo_produto is null and
	cc.cod_categoria is null
LEFT JOIN bi_categorizacao_cor cor on pc.desc_cor_produto = cor.cor
WHERE
p.grupo_produto != 'GIFTCARD'
GROUP BY
COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros'),
LTRIM(RTRIM(p.fabricante)),
v.faixa_de_desconto
HAVING
SUM(v.itens) > @min_items_for_cluster and
SUM(v.itens)/CAST(dbo.GREATEST(15,DATEDIFF(DAY,MIN(v.primeira_venda),MAX(v.ultima_venda))) AS FLOAT) IS NOT NULL;

IF OBJECT_ID('fator_base_sem_giro_errado') IS NOT NULL
  DROP TABLE fator_base_sem_giro_errado;
SELECT
b1.categoria,
b1.fabricante,
b1.faixa_de_desconto,
MAX(b1.desconto_medio) as desconto_medio,
MAX(b1.giro) as giro
INTO dbo.fator_base_sem_giro_errado
FROM fator_base b1
LEFT JOIN fator_base b2 on
	b1.categoria = b2.categoria and
	b1.fabricante = b2.fabricante and
	b1.faixa_de_desconto > b2.faixa_de_desconto
GROUP BY
b1.categoria,
b1.fabricante,
b1.faixa_de_desconto
HAVING
MAX(b1.giro) > MAX(COALESCE(b2.giro,0));

IF OBJECT_ID('fator_base_validos') IS NOT NULL
  DROP TABLE fator_base_validos;
SELECT
b1.*
INTO dbo.fator_base_validos
FROM fator_base_sem_giro_errado b1
INNER JOIN (
	SELECT
	b.categoria,
	b.fabricante
	FROM fator_base_sem_giro_errado b
	GROUP BY
	b.categoria,
	b.fabricante
	HAVING
	COUNT(*) >= @min_discount_ranges
) b2 on
	b1.categoria = b2.categoria and
	b1.fabricante = b2.fabricante;

IF OBJECT_ID('fator_cluster') IS NOT NULL
  DROP TABLE fator_cluster;
SELECT
b1.categoria,
b1.fabricante,
AVG(dbo.GREATEST(@min_factor,dbo.LEAST(@max_factor,
	LOG(b2.giro/NULLIF(b1.giro,0))/
		LOG((1-b1.desconto_medio)/CAST(NULLIF(1-b2.desconto_medio,0) as float))
))) as fator
INTO dbo.fator_cluster
FROM fator_base_validos b1
INNER JOIN (
	SELECT
	b1.*
	FROM fator_base_validos b1
	INNER JOIN (
		SELECT
		b.categoria,
		b.fabricante,
		MAX(b.faixa_de_desconto) as max_faixa_de_desconto
		FROM fator_base_validos b
		GROUP BY
		b.categoria,
		b.fabricante
	) b2 on
		b1.categoria = b2.categoria and
		b1.fabricante = b2.fabricante and
		b1.faixa_de_desconto = b2.max_faixa_de_desconto
) b2 on
	b1.categoria = b2.categoria and
	b1.fabricante = b2.fabricante and
	b1.faixa_de_desconto != b2.faixa_de_desconto
GROUP BY
b1.categoria,
b1.fabricante;

IF OBJECT_ID('produtos_categorizados') IS NOT NULL
  DROP TABLE produtos_categorizados;
SELECT
p.produto,
MAX(COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros')) as categoria,
	MAX(LTRIM(RTRIM(p.fabricante))) as fabricante
INTO dbo.produtos_categorizados
FROM produtos p
INNER JOIN produtos_precos pp on
	p.produto = pp.produto and
	pp.codigo_tab_preco = 99
LEFT JOIN bi_categorizacao_categoria ccc on
	ccc.grupo_produto = p.grupo_produto and
	ccc.cod_categoria = p.cod_categoria and
	ccc.subgrupo_produto is null and
	ccc.tipo_produto is null
LEFT JOIN bi_categorizacao_categoria cct on
	cct.grupo_produto = p.grupo_produto and
	cct.tipo_produto = p.tipo_produto and
	cct.subgrupo_produto is null and
	cct.cod_categoria is null
LEFT JOIN bi_categorizacao_categoria ccs on
	ccs.grupo_produto = p.grupo_produto and
	ccs.subgrupo_produto = p.subgrupo_produto and
	ccs.tipo_produto is null and
	ccs.cod_categoria is null
LEFT JOIN bi_categorizacao_categoria cc on
	cc.grupo_produto = p.grupo_produto and
	cc.subgrupo_produto is null and
	cc.tipo_produto is null and
	cc.cod_categoria is null
WHERE
p.grupo_produto != 'GIFTCARD'
GROUP BY
p.produto;

IF OBJECT_ID('bi_fator_produto','U') IS NOT NULL
  DROP TABLE bi_fator_produto;
CREATE TABLE dbo.bi_fator_produto (
	produto VARCHAR(50) NOT NULL,
	categoria VARCHAR(50) DEFAULT NULL,
	fabricante VARCHAR(50) DEFAULT NULL,
	nivel VARCHAR(50) DEFAULT NULL,
	fator FLOAT DEFAULT NULL,
	PRIMARY KEY (produto)
);
INSERT INTO bi_fator_produto
SELECT
p.*,
CASE
	WHEN fc1.fator IS NOT NULL THEN 'categoria-ocasiao-faixa'
	WHEN fc2.fator IS NOT NULL THEN 'categoria-ocasiao'
	WHEN fc3.fator IS NOT NULL THEN 'categoria'
	ELSE 'geral'
END as nivel,
COALESCE(fc1.fator,fc2.fator,fc3.fator,fc4.fator) as fator
FROM produtos_categorizados p
LEFT JOIN fator_cluster fc1 on
	fc1.categoria = p.categoria and
	fc1.fabricante = p.fabricante
LEFT JOIN (
	SELECT
	fc.categoria,
	AVG(fc.fator) as fator
	FROM fator_cluster fc
	GROUP BY
	fc.categoria
) fc2 on
	fc2.categoria = p.categoria
LEFT JOIN (
	SELECT
	fc.categoria,
	AVG(fc.fator) as fator
	FROM fator_cluster fc
	GROUP BY
	fc.categoria
) fc3 on
	fc3.categoria = p.categoria
CROSS JOIN (
	SELECT
	AVG(fc.fator) as fator
	FROM fator_cluster fc
) fc4;

IF OBJECT_ID('fator_base') IS NOT NULL
  DROP TABLE fator_base;
IF OBJECT_ID('fator_base_sem_giro_errado') IS NOT NULL
  DROP TABLE fator_base_sem_giro_errado;
IF OBJECT_ID('fator_base_validos') IS NOT NULL
  DROP TABLE fator_base_validos;
IF OBJECT_ID('fator_cluster') IS NOT NULL
  DROP TABLE fator_cluster;
IF OBJECT_ID('produtos_categorizados') IS NOT NULL
  DROP TABLE produtos_categorizados;