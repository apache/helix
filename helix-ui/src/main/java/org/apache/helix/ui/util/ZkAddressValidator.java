package org.apache.helix.ui.util;

import java.util.HashSet;
import java.util.Set;

public class ZkAddressValidator {

    private final Set<String> zkMachines;

    public ZkAddressValidator(Set<String> zkAddresses) {
        if (zkAddresses == null) {
            this.zkMachines = null;
        } else {
            this.zkMachines = new HashSet<String>();
            for (String zkAddress : zkAddresses) {
                for (String machine : zkAddress.split(",")) {
                    this.zkMachines.add(machine);
                }
            }
        }
    }

    public boolean validate(String zkAddress) {
        if (zkMachines == null) {
            return true;
        }

        String[] machines = zkAddress.split(",");
        for (String machine : machines) {
            if (!zkMachines.contains(machine)) {
                return false;
            }
        }

        return true;
    }
}
