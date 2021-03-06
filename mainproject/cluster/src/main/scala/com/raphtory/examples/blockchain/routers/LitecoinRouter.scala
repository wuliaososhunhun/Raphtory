package com.raphtory.examples.blockchain.routers

import com.raphtory.core.components.Router.RouterWorker
import com.raphtory.core.model.communication.EdgeAdd
import com.raphtory.core.model.communication.EdgeAddWithProperties
import com.raphtory.core.model.communication.Properties
import com.raphtory.core.model.communication.StringProperty
import com.raphtory.core.model.communication.VertexAddWithProperties
import com.raphtory.examples.blockchain.LitecoinTransaction
import spray.json.JsArray

import scala.util.hashing.MurmurHash3

class LitecoinRouter(override val routerId: Int,override val workerID:Int, val initialManagerCount: Int) extends RouterWorker {

  def parseTuple(record: Any): Unit = {
    val value        = record.asInstanceOf[LitecoinTransaction]
    val transaction  = value.transaction
    val time         = value.time
    val blockID      = value.blockID
    val block        = value.block
    val timeAsString = time.toString
    val timeAsLong   = (timeAsString.toLong) * 1000

    val txid          = transaction.asJsObject.fields("txid").toString()
    val vins          = transaction.asJsObject.fields("vin")
    val vouts         = transaction.asJsObject.fields("vout")
    val locktime      = transaction.asJsObject.fields("locktime")
    val version       = transaction.asJsObject.fields("version")
    var total: Double = 0

    for (vout <- vouts.asInstanceOf[JsArray].elements) {
      val voutOBJ = vout.asJsObject()
      var value   = voutOBJ.fields("value").toString
      total += value.toDouble
      val n            = voutOBJ.fields("n").toString
      val scriptpubkey = voutOBJ.fields("scriptPubKey").asJsObject()

      var address    = "nulldata"
      val outputType = scriptpubkey.fields("type").toString

      if (scriptpubkey.fields.contains("addresses"))
        address = scriptpubkey.fields("addresses").asInstanceOf[JsArray].elements(0).toString
      else value = "0" //TODO deal with people burning money

      //creates vertex for the receiving wallet
      sendGraphUpdate(
              VertexAddWithProperties(
                      msgTime = timeAsLong,
                      srcID = MurmurHash3.stringHash(address),
                      Properties(
                              StringProperty("type", "address"),
                              StringProperty("address", address),
                              StringProperty("outputType", outputType)
                      )
              )
      )
      //creates edge between the transaction and the wallet
      sendGraphUpdate(
              EdgeAddWithProperties(
                      msgTime = timeAsLong,
                      srcID = MurmurHash3.stringHash(txid),
                      dstID = MurmurHash3.stringHash(address),
                      Properties(StringProperty("n", n), StringProperty("value", value))
              )
      )

    }
    sendGraphUpdate(
            VertexAddWithProperties(
                    msgTime = timeAsLong,
                    srcID = MurmurHash3.stringHash(txid),
                    Properties(
                            StringProperty("type", "transaction"),
                            StringProperty("time", timeAsString),
                            StringProperty("id", txid),
                            StringProperty("total", total.toString),
                            StringProperty("lockTime", locktime.toString),
                            StringProperty("version", version.toString),
                            StringProperty("blockhash", blockID.toString),
                            StringProperty("block", block.toString)
                    )
            )
    )

    if (vins.toString().contains("coinbase")) {
      //creates the coingen node
      sendGraphUpdate(
              VertexAddWithProperties(
                      msgTime = timeAsLong,
                      srcID = MurmurHash3.stringHash("coingen"),
                      Properties(StringProperty("type", "coingen"))
              )
      )

      //creates edge between coingen and the transaction
      sendGraphUpdate(
              EdgeAdd(
                      msgTime = timeAsLong,
                      srcID = MurmurHash3.stringHash("coingen"),
                      dstID = MurmurHash3.stringHash(txid)
              )
      )
    } else
      for (vin <- vins.asInstanceOf[JsArray].elements) {
        val vinOBJ   = vin.asJsObject()
        val prevVout = vinOBJ.fields("vout").toString
        val prevtxid = vinOBJ.fields("txid").toString
        val sequence = vinOBJ.fields("sequence").toString
        //no need to create node for prevtxid as should already exist
        //creates edge between the prev transaction and current transaction
        sendGraphUpdate(
                EdgeAddWithProperties(
                        msgTime = timeAsLong,
                        srcID = MurmurHash3.stringHash(prevtxid),
                        dstID = MurmurHash3.stringHash(txid),
                        Properties(StringProperty("vout", prevVout), StringProperty("sequence", sequence))
                )
        )
      }
  }

}
