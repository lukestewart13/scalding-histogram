/**
 * Created by lukestewart on 4/9/15.
 */
import com.twitter.algebird.Aggregator
import com.twitter.scalding._

class HistogramJob(args: Args) extends Job(args) {

  //params
  val sourceFields = args("headers").split(',').toSeq.map(Symbol.apply)
  val useField = Symbol(args("field"))
  val numBuckets = args("buckets").toInt

  //source data
  val pipe = Tsv(args("input"), fields = sourceFields).read.project(useField)
  val typedPipe = TypedPipe.from[Double](pipe, fields = useField)

  //build histogram buckets
  val buckets = typedPipe aggregate {
    val minObs = Aggregator.min[Double]
    val stepBy = minObs join Aggregator.max[Double] andThenPresent { case (min, max) =>
      (max - min) / numBuckets.toDouble
    }
    minObs join stepBy andThenPresent { case (min, step) =>
      Vector.tabulate(numBuckets) { n =>
        (min + n * step) -> (min + (n + 1) * step)
      }
    }
  }

  //transform observations into buckets
  val bucketedObs = typedPipe.mapWithValue(buckets) {
    case (obs, Some(bucketList)) => bucketList find {
      case (open, close) => open <= obs && obs <= close
    } match {
      case Some(bucket) => bucket
      case _ => sys.error(s"no valid bucket range for observation: $obs")
    }
    case _ => sys.error("unexpected empty value pipe")
  }

  //count the buckets
  bucketedObs
    .groupBy(bucket => bucket)
    .size
    .write(TypedTsv(args("output")))

}
