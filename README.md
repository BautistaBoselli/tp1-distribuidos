Trabajo Practico de la materia "Sistemas Distribuidos 1" de la Universidad Nacional de Buenos Aires.

TP Multiples clientes:

Importantes:

## Cronograma

## Rabbit MQ

- [x] Rabbit: Verificar que todo este en durable
- [ ] Rabbit: Usar el amqp.Persistent (medir cambio de rendimientos)

## BullyResurrecter

- [x] Bully: Traer el bully

## Server

Siempre vivo (no recupera sesion de clientes), si se pierde conexion con el cliente pincho ese cleinte, cuando resucite habria que mandar a borrarlo

- [x] Server: calcular totales de juegos y reviews
- [x] Server: agregar Id a reviews
- [ ] Server: Finished con totales
- [ ] Server: almacenar clientes activos
- [ ] Server: ACK de reviews/games para controlar el flujo
- [ ] Server: Mandar a borrar clientes inactivos cuando termine/reconecte

## Mapper

Es el mas basico, no necesita commits para los datos recibidos o mandados.

- Solo se hace ACK de game una vez escrito en disco, si se cae antes se hace todo de vuelta (idempotente).
- Solo se hace ACK de un batch de reviews cuando se enviaron todas las reviews del batch. Si se cae va a recorrer el batch entero y mandar (no importa si se duplican porque las queries lo van a ignorar).

Hay que usar los totales como condicion para el EOF.

- [x] Mapper: generar stat con el id de la review
- [ ] Mapper: manejar state de cada MapperClient en disco (o ver como diseñarlo)
- [ ] Mapper: EOF con finished + totals para condicion de corte para reviews (googlear como seria una buena condicion de corte para worker queues).

## Queries

El gordo demo-falopa 😎

- [x] Queries: crear concepto de QueryClient
- [x] Queries: commits y temps
- [x] Queries: mover 1 y 2 a disco
- [ ] Queries: ver como chota manejar los envios (no es grave los duplicados)
- [ ] Queries: definir ids para los results
- [ ] Queries: restore commit reenvia mensaje si hace falta.

## Reducers

A priori no deberian importarle los duplicados tampoco

- [ ] Reducer: commits similares a queries supongo 🤷‍♂️
- [ ] Reducer: ver como chota manejar los envios (no es grave los duplicados)
- [ ] Reducer: ver si simplificar el envio a los ReducerClients dejando de hacer channels (medir performance)

IDs:
2 clientId
1 byte QueryId
1 byte ShardId
4 bytes ID autoincremental

## Server (respuesta)

A priori no se deberia caer nunca asi que fulbo.

- [ ] Server (respuesta): manejar duplicados de results
- [ ] Server (respuesta): enviar responses sin duplicado

## Otros

- [ ] TODOS: Pub/Sub centralizado para borrar las databases cuando se termina o corta un cliente.
- [ ] Go bubbletea para tirar los servicios
