Trabajo Practico de la materia "Sistemas Distribuidos 1" de la Universidad Nacional de Buenos Aires.

TP Multiples clientes:

Importantes:

## Rabbit MQ

- Verificar que todo este en durable
- Usar el amqp.Persistent (medir cambio de rendimientos)

## BullyResurrecter

- [ ] Traer el bully

## Server

Siempre vivo (no recupera sesion de clientes), si se pierde conexion con el cliente pincho ese cleinte, cuando resucite habria que mandar a borrarlo

- [ ] Server: calcular totales de juegos y reviews
- [ ] Server: agregar Id a reviews
- [ ] Server: ACK de reviews/games para controlar el flujo
- [ ] Server: almacenar clientes activos
- [ ] Server: Mandar a borrar clientes inactivos cuando termine/reconecte
- [ ] Server: Finished con totales

## Mapper

Es el mas basico, no necesita commits para los datos recibidos o mandados.

- Solo se hace ACK de game una vez escrito en disco, si se cae antes se hace todo de vuelta (idempotente).
- Solo se hace ACK de un batch de reviews cuando se enviaron todas las reviews del batch. Si se cae va a recorrer el batch entero y mandar (no importa si se duplican porque las queries lo van a ignorar).

Hay que usar los totales como condicion para el EOF.

- [ ] Mapper: generar stat con el id de la review
- [ ] Mapper: EOF con finished + totals para condicion de corte para reviews (googlear como seria una buena condicion de corte para worker queues).

## Queries

El gordo demo-falopa 😎

- [ ] Queries: mover 1 y 2 a disco
- [ ] Queries: ver como chota manejar los envios (no es grave los duplicados)
- [ ] Queries: definir ids para los results

## Reducers

A priori no deberian importarle los duplicados tampoco

- [ ] Reducer: commits similares a queries supongo 🤷‍♂️
- [ ] Reducer: ver como chota manejar los envios (no es grave los duplicados)

## Server (respuesta)

A priori no se deberia caer nunca asi que fulbo.

- [ ] Server (respuesta): manejar duplicados de results
- [ ] Server (respuesta): enviar responses sin duplicado

## Otros

- [ ] TODOS: Pub/Sub centralizado para borrar las databases cuando se termina o corta un cliente.
- [ ] Go bubbletea para tirar los servicios
