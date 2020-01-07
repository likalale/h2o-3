package hex.gam;

import hex.ModelBuilder;
import hex.ModelCategory;

public class GAM extends ModelBuilder<GAMModel, GAMModel.GAMParameters, GAMModel.GAMModelOutput> {

  @Override
  public ModelCategory[] can_build() { return new ModelCategory[]{ModelCategory.Regression}; }

  @Override
  public boolean isSupervised() { return true; }

  @Override
  public BuilderVisibility builderVisibility() { return BuilderVisibility.Experimental; }

  public GAM(boolean startup_once) {
    super(new GAMModel.GAMParameters(), startup_once);
  }

  public GAM(GAMModel.GAMParameters parms) {
    super(parms);
    init(false);
  }

  @Override public void init(boolean expensive) {
    super.init(expensive);
    if (expensive) {

    }
    // TODO: no validation yet - everything is allowed ;)
  }

  @Override
  public void checkDistributions() {
    if (!_response.isNumeric()) {
      error("_response", "Expected a numerical response, but instead got response with " + _response.cardinality() + " categories.");
    }
  }

  @Override
  protected boolean computePriorClassDistribution() {
    return false; // no use, we don't output probabilities
  }

  @Override
  protected int init_getNClass() {
    return 1; // only regression is supported for now
  }

  @Override
  protected Driver trainModelImpl() {
    return new GAMDriver();
  }
  
  private class GAMDriver extends Driver {
    @Override
    public void computeImpl() {
      
    }
  }
  
}
