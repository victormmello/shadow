SELECT DISTINCT fabricante FROM (
	SELECT DISTINCT
	RTRIM(p.fabricante) as fabricante
	FROM produtos p
	WHERE
	(p.grupo_produto like '[_]%' or p.grupo_produto = 'CALCADOS')
	and p.data_cadastramento >= GETDATE()-360
	
	UNION

	SELECT
	RTRIM(f.fornecedor) as fabricante
	FROM fornecedores f
	WHERE
	f.inativo = 0 and
	(f.data_para_transferencia >= GETDATE()-360)
	and f.tipo = 'PRODUTOS ACABADOS'
) t
ORDER BY t.fabricante