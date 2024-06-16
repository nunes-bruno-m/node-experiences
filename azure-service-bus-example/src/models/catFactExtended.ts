import { CatFact } from "./catFact";

export interface CatFactExtended extends CatFact {
    dateFetched: Date
}