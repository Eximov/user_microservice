const uuid = require('uuid').v4;

exports.create = data => {
    if (data.name && data.quantity) {
        let User = {
            uuid: uuid(),
            name: data.name,
            quantity: data.quantity
        }


        // Action id | timestamp | internalId | externalId | Operation
        // 1         | -         | uuid       | Tristan    | UserGenerated
        // 2         | -         | uuid       | Théo       | UserRemoved
        // 3         | -         | uuid       | Agathe     | TicketAddedToCart
        // 4         | -         | uuid       | Lisa       | TicketRemovedFromCart

    }
    else {
        // err
    }
    console.log("Creation du user avec l'id", uuid())
}