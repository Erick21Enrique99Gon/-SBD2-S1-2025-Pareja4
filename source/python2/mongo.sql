use local

db.players.find({first_name:"John"})
db.teams_denormalized.find({full_name:"John"})

db.players.aggregate([
  {
    $match: {
      first_name: "Atlanta Hawks"
    }
  },
  {
    $project: {
      _id: 1,
      name: "$first_name",
      type: { $literal: "player" },
      // Incluye otros campos que necesites
      last_name: 1,
      team: 1
    }
  },
  {
    $unionWith: {
      coll: "teams_denormalized",
      pipeline: [
        {
          $match: {
            full_name: { $regex: "Atlanta Hawks", $options: "i" }
          }
        },
        {
          $project: {
            _id: 1,
            name: "$full_name",
            type: { $literal: "team" },
            // Incluye otros campos del equipo
            abbreviation: 1,
            city: 1
          }
        }
      ]
    }
  }
])

db.victimas.find({opponent_id: 1610612738, opponent_name:'Boston Celtics'}).sort({loss_count:-1}).limit(1)

db.anotador.find()

db.perdedores.find().sort({loss_count:-1})
db.ganadores.find().sort({win_count:-1})