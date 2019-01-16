
DECLARE
	@days_from_current_date as INT;
SET @days_from_current_date = 360;


IF OBJECT_ID('disponibilidade_recebimento') IS NOT NULL
  DROP TABLE disponibilidade_recebimento;
SELECT
c.filial_a_entregar as filial,
pb.codigo_barra,
SUM(CASE pb.tamanho
	WHEN 1 THEN cp.ce1
	WHEN 2 THEN cp.ce2
	WHEN 3 THEN cp.ce3
	WHEN 4 THEN cp.ce4
	WHEN 5 THEN cp.ce5
	WHEN 6 THEN cp.ce6
	WHEN 7 THEN cp.ce7
	WHEN 8 THEN cp.ce8
	WHEN 9 THEN cp.ce9
	WHEN 10 THEN cp.ce10
	WHEN 11 THEN cp.ce11
	WHEN 12 THEN cp.ce12
	WHEN 13 THEN cp.ce13
	WHEN 14 THEN cp.ce14
	WHEN 15 THEN cp.ce15
	WHEN 16 THEN cp.ce16
	WHEN 17 THEN cp.ce17
	WHEN 18 THEN cp.ce18
	WHEN 19 THEN cp.ce19
	WHEN 20 THEN cp.ce20
	WHEN 21 THEN cp.ce21
	WHEN 22 THEN cp.ce22
	WHEN 23 THEN cp.ce23
	WHEN 24 THEN cp.ce24
	WHEN 25 THEN cp.ce25
	WHEN 26 THEN cp.ce26
	WHEN 27 THEN cp.ce27
	WHEN 28 THEN cp.ce28
	WHEN 29 THEN cp.ce29
	WHEN 30 THEN cp.ce30
	WHEN 31 THEN cp.ce31
	WHEN 32 THEN cp.ce32
	WHEN 33 THEN cp.ce33
	WHEN 34 THEN cp.ce34
	WHEN 35 THEN cp.ce35
	WHEN 36 THEN cp.ce36
	WHEN 37 THEN cp.ce37
	WHEN 38 THEN cp.ce38
	WHEN 39 THEN cp.ce39
	WHEN 40 THEN cp.ce40
	WHEN 41 THEN cp.ce41
	WHEN 42 THEN cp.ce42
	WHEN 43 THEN cp.ce43
	WHEN 44 THEN cp.ce44
	WHEN 45 THEN cp.ce45
	WHEN 46 THEN cp.ce46
	WHEN 47 THEN cp.ce47
	WHEN 48 THEN cp.ce48
	ELSE 0
END) as a_receber_prox_30
INTO dbo.disponibilidade_recebimento
FROM compras_produto cp
INNER JOIN compras c on cp.pedido = c.pedido
INNER JOIN produtos_barra pb on
		cp.produto = pb.produto and
		cp.cor_produto = pb.cor_produto
WHERE
cp.entrega BETWEEN GETDATE()-15 and GETDATE()+30
and cp.qtde_entregar > 0
and cp.QTDE_ENTREGUE = 0
and CASE pb.tamanho
	WHEN 1 THEN cp.ce1
	WHEN 2 THEN cp.ce2
	WHEN 3 THEN cp.ce3
	WHEN 4 THEN cp.ce4
	WHEN 5 THEN cp.ce5
	WHEN 6 THEN cp.ce6
	WHEN 7 THEN cp.ce7
	WHEN 8 THEN cp.ce8
	WHEN 9 THEN cp.ce9
	WHEN 10 THEN cp.ce10
	WHEN 11 THEN cp.ce11
	WHEN 12 THEN cp.ce12
	WHEN 13 THEN cp.ce13
	WHEN 14 THEN cp.ce14
	WHEN 15 THEN cp.ce15
	WHEN 16 THEN cp.ce16
	WHEN 17 THEN cp.ce17
	WHEN 18 THEN cp.ce18
	WHEN 19 THEN cp.ce19
	WHEN 20 THEN cp.ce20
	WHEN 21 THEN cp.ce21
	WHEN 22 THEN cp.ce22
	WHEN 23 THEN cp.ce23
	WHEN 24 THEN cp.ce24
	WHEN 25 THEN cp.ce25
	WHEN 26 THEN cp.ce26
	WHEN 27 THEN cp.ce27
	WHEN 28 THEN cp.ce28
	WHEN 29 THEN cp.ce29
	WHEN 30 THEN cp.ce30
	WHEN 31 THEN cp.ce31
	WHEN 32 THEN cp.ce32
	WHEN 33 THEN cp.ce33
	WHEN 34 THEN cp.ce34
	WHEN 35 THEN cp.ce35
	WHEN 36 THEN cp.ce36
	WHEN 37 THEN cp.ce37
	WHEN 38 THEN cp.ce38
	WHEN 39 THEN cp.ce39
	WHEN 40 THEN cp.ce40
	WHEN 41 THEN cp.ce41
	WHEN 42 THEN cp.ce42
	WHEN 43 THEN cp.ce43
	WHEN 44 THEN cp.ce44
	WHEN 45 THEN cp.ce45
	WHEN 46 THEN cp.ce46
	WHEN 47 THEN cp.ce47
	WHEN 48 THEN cp.ce48
	ELSE 0
END > 0
GROUP BY
c.filial_a_entregar,
pb.codigo_barra;

IF OBJECT_ID('disponibilidade_recebimento_total') IS NOT NULL
  DROP TABLE disponibilidade_recebimento_total;
SELECT
r.codigo_barra,
SUM(r.a_receber_prox_30) as a_receber_prox_30
INTO dbo.disponibilidade_recebimento_total
FROM disponibilidade_recebimento r
GROUP BY
r.codigo_barra;

IF OBJECT_ID('disponibilidade_venda') IS NOT NULL
  DROP TABLE disponibilidade_venda;
SELECT
v.filial,
v.codigo_barra,
MAX(v.produto) as produto,
MAX(v.cor_produto) as cor_produto,
MAX(v.grade) as grade,
SUM(v.itens_historico) as itens_historico,
SUM(v.itens_periodo) as itens_periodo,
SUM(v.itens_30) as itens_30
INTO dbo.disponibilidade_venda
FROM (
	-- Venda:
	SELECT
	f.filial,
	pb.codigo_barra as codigo_barra,
	MAX(pb.produto) as produto,
	MAX(pb.cor_produto) as cor_produto,
	MAX(pb.grade) as grade,
	SUM(vp.qtde) as itens_historico,
	SUM(CASE WHEN vp.data_venda > GETDATE()-@days_from_current_date THEN vp.qtde ELSE 0 END) as itens_periodo,
	SUM(CASE WHEN vp.data_venda > GETDATE()-30 THEN vp.qtde ELSE 0 END) as itens_30
	FROM loja_venda_produto vp
	INNER JOIN produtos_barra pb on vp.codigo_barra = pb.codigo_barra
	INNER JOIN filiais f on f.cod_filial = vp.codigo_filial
	WHERE
	vp.qtde > 0
	GROUP BY
	f.filial,
	pb.codigo_barra
	
	UNION

	-- Venda E-COMMMERCE:
	SELECT
	f.filial,
	pb.codigo_barra as codigo_barra,
	MAX(pb.produto) as produto,
	MAX(pb.cor_produto) as cor_produto,
	MAX(pb.grade) as grade,
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
	END) as itens_historico,
	SUM(CASE WHEN f.emissao > GETDATE()-@days_from_current_date THEN CASE pb.tamanho
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
	END ELSE 0 END) as itens_periodo,
	SUM(CASE WHEN f.emissao > GETDATE()-30 THEN CASE pb.tamanho
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
	END ELSE 0 END) as itens_30
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
	f.filial = 'e-commerce'
	GROUP BY
	f.filial,
	pb.codigo_barra
) v
GROUP BY
v.filial,
v.codigo_barra;

IF OBJECT_ID('disponibilidade_venda_total') IS NOT NULL
  DROP TABLE disponibilidade_venda_total;
SELECT
v.codigo_barra,
SUM(v.itens_historico) as itens_historico,
SUM(v.itens_periodo) as itens_periodo,
SUM(v.itens_30) as itens_30
INTO dbo.disponibilidade_venda_total
FROM disponibilidade_venda v
GROUP BY
v.codigo_barra;

IF OBJECT_ID('disponibilidade_estoque_filial') IS NOT NULL
  DROP TABLE disponibilidade_estoque_filial;
SELECT
e.filial,
e.codigo_barra,
e.produto,
e.cor_produto,
e.grade,
SUM(e.estoque_disponivel) as quantidade_estoque
INTO dbo.disponibilidade_estoque_filial
FROM w_estoque_disponivel_sku e
WHERE
e.estoque_disponivel > 0
GROUP BY
e.filial,
e.codigo_barra,
e.produto,
e.cor_produto,
e.grade;

IF OBJECT_ID('disponibilidade_estoque') IS NOT NULL
  DROP TABLE disponibilidade_estoque;
SELECT
e.codigo_barra,
e.produto,
e.cor_produto,
e.grade,
SUM(e.quantidade_estoque) as quantidade_estoque
INTO dbo.disponibilidade_estoque
FROM disponibilidade_estoque_filial e
GROUP BY
e.codigo_barra,
e.produto,
e.cor_produto,
e.grade;

IF OBJECT_ID('disponibilidade_venda_categoria_tamanho') IS NOT NULL
  DROP TABLE disponibilidade_venda_categoria_tamanho;
SELECT
COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros') as categoria,
v.grade,
SUM(v.itens_periodo) as itens_periodo_categoria_tamanho
INTO dbo.disponibilidade_venda_categoria_tamanho
FROM disponibilidade_venda v
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
WHERE
p.grupo_produto != 'GIFTCARD'
GROUP BY
COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros'),
v.grade;

IF OBJECT_ID('disponibilidade_venda_produto_barra_categoria_tamanho') IS NOT NULL
  DROP TABLE disponibilidade_venda_produto_barra_categoria_tamanho;
SELECT
pb.codigo_barra,
pb.produto,
pb.cor_produto,
pb.grade,
vct.itens_periodo_categoria_tamanho,
dvt.itens_historico as itens_historico_total,
dvt.itens_30 as itens_30_total,
e.quantidade_estoque as quantidade_estoque_total,
drt.a_receber_prox_30 as a_receber_prox_30_total
INTO dbo.disponibilidade_venda_produto_barra_categoria_tamanho
FROM produtos_barra pb
INNER JOIN produtos p on p.produto = pb.produto
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
INNER JOIN disponibilidade_venda_categoria_tamanho vct on
	vct.categoria = COALESCE(ccc.categoria,cct.categoria,ccs.categoria,cc.categoria,'Outros') and
	vct.grade = pb.grade
LEFT JOIN disponibilidade_venda_total dvt on dvt.codigo_barra = pb.codigo_barra
LEFT JOIN disponibilidade_recebimento_total drt on drt.codigo_barra = pb.codigo_barra
LEFT JOIN disponibilidade_estoque e on e.codigo_barra = pb.codigo_barra
WHERE
p.grupo_produto != 'GIFTCARD';

IF OBJECT_ID('disponibilidade_venda_produto_barra_categoria') IS NOT NULL
  DROP TABLE disponibilidade_venda_produto_barra_categoria;
SELECT
ct.produto,
ct.cor_produto,
SUM(ct.itens_periodo_categoria_tamanho) as itens_periodo_categoria
INTO dbo.disponibilidade_venda_produto_barra_categoria
FROM disponibilidade_venda_produto_barra_categoria_tamanho ct
GROUP BY
ct.produto,
ct.cor_produto;

IF OBJECT_ID('bi_disponibilidade_produto_barra') IS NOT NULL
  DROP TABLE bi_disponibilidade_produto_barra;
CREATE TABLE dbo.bi_disponibilidade_produto_barra (
	filial VARCHAR(50) DEFAULT NULL,
	codigo_barra VARCHAR(50) DEFAULT NULL,
	produto VARCHAR(50) DEFAULT NULL,
	cor_produto VARCHAR(50) DEFAULT NULL,
	grade VARCHAR(50) DEFAULT NULL,
	itens_historico_filial INT DEFAULT NULL,
	itens_historico_total INT DEFAULT NULL,
	itens_30_filial INT DEFAULT NULL,
	itens_30_total INT DEFAULT NULL,
	quantidade_estoque_filial INT DEFAULT NULL,
	quantidade_estoque_total INT DEFAULT NULL,
	a_receber_prox_30_filial INT DEFAULT NULL,
	a_receber_prox_30_total INT DEFAULT NULL,
	peso_disponibilidade FLOAT DEFAULT NULL,
	disponibilidade_filial FLOAT DEFAULT NULL,
	disponibilidade_total FLOAT DEFAULT NULL,
	PRIMARY KEY (filial,codigo_barra)
);
INSERT INTO dbo.bi_disponibilidade_produto_barra
SELECT
f.filial,
pb.codigo_barra,
pb.produto,
pb.cor_produto,
pb.grade,
COALESCE(v.itens_historico,0) as itens_historico_filial,
COALESCE(ct.itens_historico_total,0) as itens_historico_total,
COALESCE(v.itens_30,0) as itens_30_filial,
COALESCE(ct.itens_30_total,0) as itens_30_total,
COALESCE(ef.quantidade_estoque,0) as quantidade_estoque_filial,
COALESCE(ct.quantidade_estoque_total,0) as quantidade_estoque_total,
COALESCE(r.a_receber_prox_30,0) as a_receber_prox_30_filial,
COALESCE(ct.a_receber_prox_30_total,0) as a_receber_prox_30_total,
COALESCE(ct.itens_periodo_categoria_tamanho/NULLIF(CAST(c.itens_periodo_categoria AS FLOAT),0),0) as peso_disponibilidade,
CASE WHEN ef.quantidade_estoque > 0 THEN COALESCE(ct.itens_periodo_categoria_tamanho/NULLIF(CAST(c.itens_periodo_categoria AS FLOAT),0),0) ELSE 0 END as disponibilidade_filial,
CASE WHEN ct.quantidade_estoque_total > 0 THEN COALESCE(ct.itens_periodo_categoria_tamanho/NULLIF(CAST(c.itens_periodo_categoria AS FLOAT),0),0) ELSE 0 END as disponibilidade_total
-- INTO dbo.bi_disponibilidade_produto_barra
FROM produtos_barra pb
INNER JOIN filiais f on
	f.filial IN ('iguatemi campinas','santa ursula','mm dom pedro','premium outlet itupeva','santos praiamar','piracicaba shopping','jundiai shopping','e-commerce')
INNER JOIN produtos p on p.produto = pb.produto
LEFT JOIN disponibilidade_venda_produto_barra_categoria_tamanho ct on ct.codigo_barra = pb.codigo_barra
LEFT JOIN disponibilidade_venda_produto_barra_categoria c on
	c.produto = pb.produto and
	c.cor_produto = pb.cor_produto
LEFT JOIN disponibilidade_estoque_filial ef on ef.codigo_barra = pb.codigo_barra  and ef.filial = f.filial
LEFT JOIN disponibilidade_venda v on v.codigo_barra = pb.codigo_barra and v.filial = f.filial
LEFT JOIN disponibilidade_recebimento r on r.codigo_barra = pb.codigo_barra and r.filial = f.filial
WHERE
p.grupo_produto != 'GIFTCARD';

IF OBJECT_ID('bi_disponibilidade_produto_cor') IS NOT NULL
  DROP TABLE bi_disponibilidade_produto_cor;
CREATE TABLE dbo.bi_disponibilidade_produto_cor (
	filial VARCHAR(50) DEFAULT NULL,
	produto VARCHAR(50) DEFAULT NULL,
	cor_produto VARCHAR(50) DEFAULT NULL,
	itens_historico_filial INT DEFAULT NULL,
	itens_historico_total INT DEFAULT NULL,
	itens_30_filial INT DEFAULT NULL,
	itens_30_total INT DEFAULT NULL,
	quantidade_estoque_filial INT DEFAULT NULL,
	quantidade_estoque_total INT DEFAULT NULL,
	a_receber_prox_30_filial INT DEFAULT NULL,
	a_receber_prox_30_total INT DEFAULT NULL,
	peso_disponibilidade FLOAT DEFAULT NULL,
	disponibilidade_filial FLOAT DEFAULT NULL,
	disponibilidade_total FLOAT DEFAULT NULL,
	PRIMARY KEY (filial,produto,cor_produto)
);
INSERT INTO dbo.bi_disponibilidade_produto_cor
SELECT
dpb.filial,
dpb.produto,
dpb.cor_produto,
SUM(dpb.itens_historico_filial) as itens_historico_filial,
SUM(dpb.itens_historico_total) as itens_historico_total,
SUM(dpb.itens_30_filial) as itens_30_filial,
SUM(dpb.itens_30_total) as itens_30_total,
SUM(dpb.quantidade_estoque_filial) as quantidade_estoque_filial,
SUM(dpb.quantidade_estoque_total) as quantidade_estoque_total,
SUM(dpb.a_receber_prox_30_filial) as a_receber_prox_30_filial,
SUM(dpb.a_receber_prox_30_total) as a_receber_prox_30_total,
SUM(dpb.peso_disponibilidade) as peso_disponibilidade,
SUM(dpb.disponibilidade_filial) as disponibilidade_filial,
SUM(dpb.disponibilidade_total) as disponibilidade_total
-- INTO dbo.bi_disponibilidade_produto_cor
FROM bi_disponibilidade_produto_barra dpb
GROUP BY
dpb.filial,
dpb.produto,
dpb.cor_produto;

IF OBJECT_ID('disponibilidade_recebimento') IS NOT NULL
  DROP TABLE disponibilidade_recebimento;
IF OBJECT_ID('disponibilidade_recebimento_total') IS NOT NULL
  DROP TABLE disponibilidade_recebimento_total;
IF OBJECT_ID('disponibilidade_venda') IS NOT NULL
  DROP TABLE disponibilidade_venda;
IF OBJECT_ID('disponibilidade_venda_total') IS NOT NULL
  DROP TABLE disponibilidade_venda_total;
IF OBJECT_ID('disponibilidade_estoque_filial') IS NOT NULL
  DROP TABLE disponibilidade_estoque_filial;
IF OBJECT_ID('disponibilidade_estoque') IS NOT NULL
  DROP TABLE disponibilidade_estoque;
IF OBJECT_ID('disponibilidade_venda_categoria_tamanho') IS NOT NULL
  DROP TABLE disponibilidade_venda_categoria_tamanho;
IF OBJECT_ID('disponibilidade_venda_produto_barra_categoria_tamanho') IS NOT NULL
  DROP TABLE disponibilidade_venda_produto_barra_categoria_tamanho;
IF OBJECT_ID('disponibilidade_venda_produto_barra_categoria') IS NOT NULL
  DROP TABLE disponibilidade_venda_produto_barra_categoria;
