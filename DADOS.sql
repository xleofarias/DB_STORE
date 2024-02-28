SELECT * FROM pedidos;

SELECT * FROM usuarios;

SELECT * FROM produtos;

SELECT
    pedidos.DATA_PEDIDO,
	pedidos.ID_PEDIDO,
    usuarios.NOME,
    usuarios.SOBRENOME,
    produtos.ID_PRODUTO,
    produtos.TITULO,
    pedidos.QUANTIDADE,
    produtos.PRECO,
    CAST(pedidos.QUANTIDADE * produtos.PRECO AS DECIMAL(5, 2)) AS COMPRA_VALOR_TOTAL
FROM 
	pedidos
    INNER JOIN
		produtos ON pedidos.ID_PRODUTO = produtos.ID_PRODUTO
	INNER JOIN
		usuarios ON pedidos.USER_ID = usuarios.USER_ID
ORDER BY
	DATA_PEDIDO